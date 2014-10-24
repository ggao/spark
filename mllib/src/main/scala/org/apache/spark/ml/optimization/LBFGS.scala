/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.ml.optimization

import org.apache.spark.ml.optimization.GradientDescent._
import org.apache.spark.mllib.linalg.BLAS._

import scala.collection.mutable.ArrayBuffer

import breeze.linalg.{DenseVector => BDV}
import breeze.optimize.{CachedDiffFunction, DiffFunction}
import breeze.optimize.{LBFGS => BreezeLBFGS, OWLQN => BreezeOWLQN}

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Vectors, Vector}

/**
 * :: DeveloperApi ::
 * Class used to solve an optimization problem using Limited-memory BFGS.
 * Reference: [[http://en.wikipedia.org/wiki/Limited-memory_BFGS]]
 * @param gradient Gradient function to be used.
 * @param regularizer Regularizer to be used for regularization.
 */
private[spark]
class LBFGS (private var gradient: Gradient, private var regularizer: Regularizer)
  extends Optimizer with Logging {

  private var numCorrections = 10
  private var convergenceTol = 1E-4
  private var maxNumIterations = 100

  /**
   * Set the number of corrections used in the LBFGS update. Default 10.
   * Values of numCorrections less than 3 are not recommended; large values
   * of numCorrections will result in excessive computing time.
   * 3 < numCorrections < 10 is recommended.
   * Restriction: numCorrections > 0
   */
  def setNumCorrections(corrections: Int): this.type = {
    assert(corrections > 0)
    this.numCorrections = corrections
    this
  }

  /**
   * Set the convergence tolerance of iterations for L-BFGS. Default 1E-4.
   * Smaller value will lead to higher accuracy with the cost of more iterations.
   */
  def setConvergenceTol(tolerance: Double): this.type = {
    this.convergenceTol = tolerance
    this
  }

  /**
   * Set the maximal number of iterations for L-BFGS. Default 100.
   */
  def setNumIterations(iters: Int): this.type = {
    this.maxNumIterations = iters
    this
  }

  /**
   * Set the gradient function (of the loss function of one single data example)
   * to be used for L-BFGS.
   */
  def setGradient(gradient: Gradient): this.type = {
    this.gradient = gradient
    this
  }

  override def optimize(data: RDD[(Double, Vector)], initialWeights: Vector): Vector = {
    val (weights, _) = LBFGS.runLBFGS(
      data,
      gradient,
      regularizer,
      numCorrections,
      convergenceTol,
      maxNumIterations,
      initialWeights)
    weights
  }

}

/**
 * :: DeveloperApi ::
 * Top-level method to run L-BFGS.
 */
private[spark]
object LBFGS extends Logging {
  /**
   * Run Limited-memory BFGS (L-BFGS) in parallel.
   * Averaging the subgradients over different partitions is performed using one standard
   * spark map-reduce in each iteration.
   *
   * @param data - Input data for L-BFGS. RDD of the set of data examples, each of
   *               the form (label, [feature values]).
   * @param gradient - Gradient object (used to compute the gradient of the loss function of
   *                   one single data example)
   * @param regularizer - Updater function to actually perform a gradient step in a given direction.
   * @param numCorrections - The number of corrections used in the L-BFGS update.
   * @param convergenceTol - The convergence tolerance of iterations for L-BFGS
   * @param maxNumIterations - Maximal number of iterations that L-BFGS can be run.
   *
   * @return A tuple containing two elements. The first element is a column matrix containing
   *         weights for every feature, and the second element is an array containing the loss
   *         computed for every iteration.
   */
  def runLBFGS(
      data: RDD[(Double, Vector)],
      gradient: Gradient,
      regularizer: Regularizer,
      numCorrections: Int,
      convergenceTol: Double,
      maxNumIterations: Int,
      initialWeights: Vector): (Vector, Array[Double]) = {

    val lossHistory = new ArrayBuffer[Double](maxNumIterations)

    val numExamples = data.count()

    // if no data, return initial weights to avoid NaNs
    if (numExamples == 0) {
      logInfo("LBFGS.runLBFGS returning initial weights, no data found")
      return (initialWeights, lossHistory.toArray)
    }

    val costFun = new CostFun(data, gradient, regularizer, numExamples)

    val lbfgs = new BreezeLBFGS[BDV[Double]](maxNumIterations, numCorrections, convergenceTol)

    val states =
      lbfgs.iterations(new CachedDiffFunction(costFun), initialWeights.toBreeze.toDenseVector)

    /**
     * NOTE: lossSum and loss is computed using the weights from the previous iteration
     * and regVal is the regularization value computed in the previous iteration as well.
     */
    var state = states.next()
    while(states.hasNext) {
      lossHistory.append(state.value)
      state = states.next()
    }
    lossHistory.append(state.value)
    val weights = Vectors.fromBreeze(state.x)

    logInfo("LBFGS.runLBFGS finished. Last 10 losses %s".format(
      lossHistory.takeRight(10).mkString(", ")))

    (weights, lossHistory.toArray)
  }

  /**
   * CostFun implements Breeze's DiffFunction[T], which returns the loss and gradient
   * at a particular point (weights). It's used in Breeze's convex optimization routines.
   */
  private class CostFun(
    data: RDD[(Double, Vector)],
    gradient: Gradient,
    regularizer: Regularizer,
    numExamples: Long) extends DiffFunction[BDV[Double]] {

    override def calculate(weights: BDV[Double]): (Double, BDV[Double]) = {
      // Have a local copy to avoid the serialization of CostFun object which is not serializable.
      val w = Vectors.fromBreeze(weights)
      val n = w.size
      val bcW = data.context.broadcast(w)
      val localGradient = gradient

      val (gradientSum, lossSum) = data.aggregate((Vectors.zeros(n), 0.0))(
          seqOp = (c, v) => (c, v) match { case ((grad, loss), (label, features)) =>
            val l = localGradient.compute(
              features, label, bcW.value, grad)
            (grad, loss + l)
          },
          combOp = (c1, c2) => (c1, c2) match { case ((grad1, loss1), (grad2, loss2)) =>
            axpy(1.0, grad2, grad1)
            (grad1, loss1 + loss2)
          })

      gradientSum.toBreeze :*= (1.0 / numExamples)

      val loss = lossSum / numExamples +
        regularizer.compute(Vectors.fromBreeze(weights), gradientSum)

      (loss, gradientSum.toBreeze.asInstanceOf[BDV[Double]])
    }
  }
}