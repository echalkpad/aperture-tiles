/*
 * Copyright (c) 2014 Oculus Info Inc.
 * http://www.oculusinfo.com/
 *
 * Released under the MIT License.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is furnished to do
 * so, subject to the following conditions:

 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.

 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.oculusinfo.tilegen.datasets



import scala.util.Try

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import com.oculusinfo.binning.TileData
import com.oculusinfo.binning.impl.AOITilePyramid
import com.oculusinfo.binning.impl.WebMercatorTilePyramid
import com.oculusinfo.binning.io.serialization.TileSerializer
import com.oculusinfo.tilegen.spark.DoubleMaxAccumulatorParam
import com.oculusinfo.tilegen.spark.DoubleMinAccumulatorParam
import com.oculusinfo.tilegen.tiling.IndexScheme
import com.oculusinfo.tilegen.tiling.analytics.AnalysisDescription

import scala.reflect.ClassTag

import com.oculusinfo.tilegen.tiling.analytics.BinningAnalytic
import org.slf4j.LoggerFactory



trait DataDescriptor[T]
{
	def parseRecords (iter: Iterator[T], fields: String*): Iterator[(T, Try[Map[String, Any]])]
}



/**
 * A description of how to extract an index from the data
 */
trait IndexDescriptor[IT]
{
	def description: String

	def indexScheme: IndexScheme[IT]

	def fields: Array[String]

	def calculateIndex (values: Map[String, Any]): IT
}



/**
 * A description of how to extract a record value from the data
 */
trait ValueDescriptor[PT, BT]
{
	def serializer: TileSerializer[BT]

	def binningAnalytic: BinningAnalytic[PT, BT]

	def fields: Array[String]

	def calculateValue (values: Map[String, Any]): PT
}



/**
 * Created by nkronenfeld on 12/10/2014.
 */
object StandardDataset
{
	private val Logger = LoggerFactory.getLogger(classOf[StandardDataset[_, _, _, _, _, _]])
}



abstract class StandardDataset[RT: ClassTag, IT: ClassTag, PT: ClassTag, DT: ClassTag, AT: ClassTag, BT]
(dataDescriptor: DataDescriptor[RT],
 indexDescriptor: IndexDescriptor[IT],
 valueDescriptor: ValueDescriptor[PT, BT],
 dataAnalytics: Option[AnalysisDescription[(IT, PT), DT]],
 tileAnalytics: Option[AnalysisDescription[TileData[BT], AT]],
 levels: Seq[Seq[Int]],
 tileWidth: Int,
 tileHeight: Int,
 config: DatasetConfiguration)
		extends Dataset[IT, PT, DT, AT, BT]
{
	private lazy val consolidationPartitions =
		config.getIntOption("oculus.binning.consolidationPartitions",
			                   "The number of partitions into which to consolidate data when done")

	// We don't actually define this one
	def getName: String = "undefined"

	def getDescription =
		config.getStringOption("oculus.binning.description",
			                      "The description to put in the tile metadata")
		.getOrElse("Binned " + getName + " data showing " + indexDescriptor.description)

	def getLevels = levels

	def getTilePyramid = {
		val projection = config.getString("oculus.binning.projection",
			                                 "The type of tile pyramid to use",
			                                 Some("EPSG:4326"))
		if ("EPSG:900913" == projection) {
			new WebMercatorTilePyramid()
		} else {
			val autoBounds = config.getBoolean("oculus.binning.projection.autobounds",
				                                  "If true, calculate tile pyramid bounds automatically; " +
				                                  "if false, use values given by properties",
				                                  Some(true))

			val (minX, maxX, minY, maxY) =
				if (autoBounds) {
					getAxisBounds
				} else {
					(config.getDoubleOption("oculus.binning.projection.minx",
						                       "The minimum x value to use for " +
						                       "the tile pyramid").get,
							config.getDoubleOption("oculus.binning.projection.maxx",
								                      "The maximum x value to use for " +
								                      "the tile pyramid").get,
							config.getDoubleOption("oculus.binning.projection.miny",
								                      "The minimum y value to use for " +
								                      "the tile pyramid").get,
							config.getDoubleOption("oculus.binning.projection.maxy",
								                      "The maximum y value to use for " +
								                      "the tile pyramid").get)
				}
			new AOITilePyramid(minX, minY, maxX, maxY)
		}
	}

	override def getNumXBins = tileWidth

	override def getNumYBins = tileHeight

	override def getConsolidationPartitions: Option[Int] = consolidationPartitions

	def getIndexScheme = indexDescriptor.indexScheme

	def getTileSerializer = valueDescriptor.serializer

	def getBinningAnalytic = valueDescriptor.binningAnalytic

	def getDataAnalytics = dataAnalytics

	def getTileAnalytics = tileAnalytics



	/**
	 * Calculate the bounds of our data
	 */
	private def getAxisBounds (): (Double, Double, Double, Double) = {
		val localIndexer = indexDescriptor
		val cartesianConversion = localIndexer.indexScheme.toCartesian(_)
		val toCartesian: RDD[(IT, PT, Option[DT])] => RDD[(Double, Double)] =
			rdd => rdd.map(_._1).map(cartesianConversion)
		val coordinates = transformRDD(toCartesian)

		// Figure out our axis bounds
		val minXAccum = coordinates.context.accumulator(Double.MaxValue)(new DoubleMinAccumulatorParam)
		val maxXAccum = coordinates.context.accumulator(Double.MinValue)(new DoubleMaxAccumulatorParam)
		val minYAccum = coordinates.context.accumulator(Double.MaxValue)(new DoubleMinAccumulatorParam)
		val maxYAccum = coordinates.context.accumulator(Double.MinValue)(new DoubleMaxAccumulatorParam)

		config.setDistributedComputation(true)
		coordinates.foreach(p => {
			val (x, y) = p
			minXAccum += x
			maxXAccum += x
			minYAccum += y
			maxYAccum += y
		}
		                   )
		config.setDistributedComputation(false)

		val minX = minXAccum.value
		val maxX = maxXAccum.value
		val minY = minYAccum.value
		val maxY = maxYAccum.value

		// Include a fraction of a bin extra in the bounds, so the max goes on the
		// right side of the last tile, rather than forming an extra tile.
		val maxLevel = {
			if (levels.isEmpty) 18
			else levels.map(_.reduce(_ max _)).reduce(_ max _)
		}
		val epsilon = (1.0 / (1 << maxLevel))
		val adjustedMaxX = maxX + (maxX - minX) * epsilon / (tileWidth * tileWidth)
		val adjustedMaxY = maxY + (maxY - minY) * epsilon / (tileHeight * tileHeight)
		StandardDataset.Logger.debug("Dataset {}: Got bounds {} to {} ({}) x, {} to {} ({}) y",
			                            Array(getName, minX, maxX, adjustedMaxX, minY, maxY, adjustedMaxY)
			                            .asInstanceOf[Array[Object]])

		(minX, adjustedMaxX, minY, adjustedMaxY)
	}
}



class StaticStandardDataset[RT: ClassTag, IT: ClassTag, PT: ClassTag, DT: ClassTag, AT: ClassTag, BT]
(rawData: RDD[RT],
 dataDescriptor: DataDescriptor[RT],
 indexDescriptor: IndexDescriptor[IT],
 valueDescriptor: ValueDescriptor[PT, BT],
 dataAnalytics: Option[AnalysisDescription[(IT, PT), DT]],
 tileAnalytics: Option[AnalysisDescription[TileData[BT], AT]],
 levels: Seq[Seq[Int]],
 tileWidth: Int,
 tileHeight: Int,
 config: DatasetConfiguration)
		extends StandardDataset[RT, IT, PT, DT, AT, BT](dataDescriptor, indexDescriptor, valueDescriptor, dataAnalytics,
			                                               tileAnalytics, levels, tileWidth, tileHeight, config)
{
	type STRATEGY_TYPE = StaticStandardProcessingStrategy[RT, IT, PT, DT]
	override protected var strategy: STRATEGY_TYPE = null

	def initialize (sc: SparkContext): Unit =
		initialize(new StaticStandardProcessingStrategy[RT, IT, PT, DT](sc, rawData, dataDescriptor, indexDescriptor,
			                                                               valueDescriptor, dataAnalytics, config))
}



class StaticStandardProcessingStrategy[RT: ClassTag, IT: ClassTag, PT: ClassTag, DT: ClassTag]
(sc: SparkContext,
 rawData: RDD[RT],
 dataDescriptor: DataDescriptor[RT],
 indexDescriptor: IndexDescriptor[IT],
 valueDescriptor: ValueDescriptor[PT, _],
 dataAnalytics: Option[AnalysisDescription[(IT, PT), DT]],
 config: DatasetConfiguration)
		extends StaticProcessingStrategy[IT, PT, DT](sc)
{
	override protected def getData: RDD[(IT, PT, Option[DT])] = {
		val localConfig = config
		val localParser = dataDescriptor
		val localIndexer = indexDescriptor
		val localValuer = valueDescriptor
		val localDataAnalytics = dataAnalytics
		// Determine which fields we need
		val fields = localIndexer.fields ++ localValuer.fields

		rawData.mapPartitions(iter =>
			localParser
			.parseRecords(iter, fields: _*)
			.map(_._2) // We don't need the original record (in _1)
		                     )
		// Filter out unsuccessful parsings
		.filter(r => r.isSuccess).map(_.get)
		.map(fieldValues =>
			Try((localIndexer.calculateIndex(fieldValues),
					localValuer.calculateValue(fieldValues)))
		    ).filter(_.isSuccess).map(_.get)
		.map { case (index, value) =>
			// Run any data analytics
			(index, value, localDataAnalytics.map(_.convert((index, value))))
		}
	}


	override def getDataAnalytics: Option[AnalysisDescription[_, DT]] = dataAnalytics
}