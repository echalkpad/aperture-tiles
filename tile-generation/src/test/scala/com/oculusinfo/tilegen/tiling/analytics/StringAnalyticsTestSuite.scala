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

package com.oculusinfo.tilegen.tiling.analytics



import java.lang.{Double => JavaDouble}

import scala.collection.JavaConverters._

import org.scalatest.FunSuite



class StringAnalyticsTestSuite extends FunSuite {
	test("String Score Analytic") {
		val a = Map("a" -> 1.0, "b" -> 2.0, "c" -> 3.0, "d" -> 4.0)
		val b = Map("a" -> 5.0, "b" -> 4.0, "c" -> 3.0, "d" -> 2.0, "e" -> 1.0)

		val analytic = new StringScoreAnalytic[Double](new NumericSumAnalytic[Double]())
		assert(Map("a" -> 6.0, "b" -> 6.0, "c" -> 6.0, "d" -> 6.0, "e" -> 1.0) ===
			       analytic.aggregate(a, b))
	}

	test("String Score Tile Analytic") {
		val a = Map("a" -> 1.0, "b" -> 2.0, "c" -> 3.0, "d" -> 4.0)
		val b = Map("a" -> 5.0, "b" -> 4.0, "c" -> 3.0, "d" -> 2.0, "e" -> 1.0)

		val analytic = new StringScoreTileAnalytic[Double](Some("test"), new NumericSumTileAnalytic[Double]())
		println("value: \""+analytic.valueToString(a)+"\"")
		assert("[\"a\":1.0,\"b\":2.0,\"c\":3.0,\"d\":4.0]" === analytic.valueToString(a))
	}

	test("String score processing limits") {
		val a1 = new StringScoreAnalytic[Double](new NumericSumAnalytic[Double](), Some(5), Some(_._2 < _._2))
		val a2 = new StringScoreAnalytic[Double](new NumericSumAnalytic[Double](), Some(5), Some(_._2 > _._2))

		val a = Map("a" -> 1.0, "b" -> 2.0, "c" -> 3.0, "d" -> 4.0)
		val b = Map("c" -> 1.0, "d" -> 2.0, "e" -> 5.0, "f" -> 0.0)

		assert(Map("f" -> 0.0, "a" -> 1.0, "b" -> 2.0, "c" -> 4.0, "e" -> 5.0)
			       === a1.aggregate(a, b))
		assert(Map("a" -> 1.0, "b" -> 2.0, "c" -> 4.0, "e" -> 5.0, "d" -> 6.0)
			       === a2.aggregate(a, b))
	}

	test("String score storage limits") {
		val ba1 = new StringScoreBinningAnalytic[Double, JavaDouble](
			new NumericSumBinningAnalytic[Double, JavaDouble](),
			Some(5),
			Some(_._2 < _._2),
			Some(3))
		val ba2 = new StringScoreBinningAnalytic[Double, JavaDouble](
			new NumericSumBinningAnalytic[Double, JavaDouble](),
			Some(5),
			Some(_._2 > _._2),
			Some(3))

		val a = Map("a" -> 1.0, "b" -> 2.0, "c" -> 3.0, "d" -> 4.0)

		assert(List(("a", 1.0), ("b", 2.0), ("c", 3.0)) ===
			       ba1.finish(a).asScala
			       .map(p => (p.getFirst, p.getSecond.doubleValue)))
		assert(List(("d", 4.0), ("c", 3.0), ("b", 2.0)) ===
			       ba2.finish(a).asScala
			       .map(p => (p.getFirst, p.getSecond.doubleValue)))
	}
}
