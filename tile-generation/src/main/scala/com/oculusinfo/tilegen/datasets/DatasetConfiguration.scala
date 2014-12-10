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



import java.util.Properties

import scala.collection.JavaConverters._

import com.oculusinfo.tilegen.util.PropertiesWrapper



/**
 *
 * Created by nkronenfeld on 12/10/2014.
 */
abstract class DatasetConfiguration (properties: Properties) extends PropertiesWrapper(properties) {
  def fields: Seq[String]
  def fieldType (field: String): String
}

class CSVDatasetConfiguration (properties: Properties) extends DatasetConfiguration(properties) {
  private val CSVFields = {
    val indexProps = properties.stringPropertyNames.asScala.toSeq
      .filter(_.startsWith("oculus.binning.parsing."))
      .filter(_.endsWith(".index"))


    val orderedProps = indexProps.map(prop => (prop, properties.getProperty(prop).toInt))
      .sortBy(_._2).map(_._1)

    orderedProps.map(property =>
      property.substring("oculus.binning.parsing.".length, property.length-".index".length)
    )
  }
  private val fieldIndices =
    Range(0, fields.size).map(n => (fields(n) -> n)).toMap

  def fields = CSVFields
  def fieldType (field: String): String =
        getString("oculus.binning.parsing."+field+".fieldType",
          "The type of the "+field+" field",
          Some(if ("constant" == field || "zero" == field) "constant"
          else ""))
}