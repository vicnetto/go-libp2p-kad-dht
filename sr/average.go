package sr

import (
	"github.com/Xeway/bigmath"
	"math"
	"math/big"
)

var keySpace = 255
var alpha = 0.1
var beta = 0.0

func SetParameters(alphaParam float64, betaParam float64) {
	alpha = alphaParam
	beta = betaParam
}

type MeanType int

const (
	Mean MeanType = iota
	MeanStdDev
	WeightedMean
	WeightedMeanStdDev
	CPL
)

var LastMeanType = CPL

func (mt MeanType) String() string {
	switch mt {
	case Mean:
		return "M"
	case MeanStdDev:
		return "M+STD"
	case WeightedMean:
		return "W"
	case WeightedMeanStdDev:
		return "W+STD"
	case CPL:
		return "CPL"
	default:
		return "Unknown"
	}
}

type WelfordAverage struct {
	count *big.Int

	mean, sumDeltaMean                 *big.Float
	weightedMean, sumDeltaWeightedMean *big.Float
	cplSum                             int

	errorSquared *big.Float
}

// NewWelfordMovingAverage initializes a new WelfordAverage instance
func NewWelfordMovingAverage() *WelfordAverage {
	return &WelfordAverage{
		count:                big.NewInt(0),
		sumDeltaMean:         big.NewFloat(0),
		sumDeltaWeightedMean: big.NewFloat(0),
		mean:                 big.NewFloat(0),
		weightedMean:         big.NewFloat(0),
		errorSquared:         big.NewFloat(0),
	}
}

// NewWelfordMovingAverageFromMean initializes a new WelfordAverage from mean
func NewWelfordMovingAverageFromMean(average WelfordAverage) *WelfordAverage {
	newAverage := NewWelfordMovingAverage()
	newAverage.Add(average.GetAverage(Mean))
	return newAverage
}

func (w *WelfordAverage) GetAverage(meanType MeanType) *big.Int {
	switch meanType {
	case Mean:
		mean, _ := w.mean.Int(new(big.Int))
		return mean
	case MeanStdDev:
		return w.getAverageWithStdDev(MeanStdDev)
	case WeightedMean:
		weightedMean, _ := w.weightedMean.Int(new(big.Int))
		return weightedMean
	case WeightedMeanStdDev:
		return w.getAverageWithStdDev(WeightedMeanStdDev)
	case CPL:
		return big.NewInt(int64(w.getCPL()))
	default:
		return nil
	}
}

func (w *WelfordAverage) getAverageWithStdDev(meanType MeanType) *big.Int {
	std := w.GetStdDev(meanType)
	rightFactor := big.NewFloat(float64(beta))
	right := new(big.Float).Mul(rightFactor, std)

	var average *big.Int
	switch meanType {
	case Mean, MeanStdDev:
		average, _ = new(big.Float).Add(w.mean, right).Int(new(big.Int))
	case WeightedMean, WeightedMeanStdDev:
		average, _ = new(big.Float).Add(w.weightedMean, right).Int(new(big.Int))
	default:
		return nil
	}
	return average
}

// getVariance returns the current variance
func (w *WelfordAverage) getVariance(meanType MeanType) *big.Float {
	if w.count.Cmp(big.NewInt(2)) < 0 {
		return big.NewFloat(0.0) // Variance is undefined for less than 2 samples
	}
	countMinusOne := new(big.Float).SetInt(new(big.Int).Sub(w.count, big.NewInt(1)))

	switch meanType {
	case Mean, MeanStdDev:
		return new(big.Float).Quo(w.sumDeltaMean, countMinusOne)
	case WeightedMean, WeightedMeanStdDev:
		return new(big.Float).Quo(w.sumDeltaWeightedMean, countMinusOne)
	default:
		return nil
	}
}

// GetStdDev returns the sample standard deviation
func (w *WelfordAverage) GetStdDev(meanType MeanType) *big.Float {
	return new(big.Float).Sqrt(w.getVariance(meanType))
}

func (w *WelfordAverage) GetStdDevAsInt(meanType MeanType) *big.Int {
	stdDev, _ := w.GetStdDev(meanType).Int(new(big.Int))
	return stdDev
}

func (w *WelfordAverage) getCPL() int {
	return int(math.Round(float64(w.cplSum) / float64(w.count.Int64())))
}

// Add adds a new value to the dataset and updates the mean and variance
func (w *WelfordAverage) Add(value *big.Int) {
	w.count.Add(w.count, big.NewInt(1))
	valueFloat := new(big.Float).SetInt(value)
	countFloat := new(big.Float).SetInt(w.count)

	// For the error
	if w.count.Cmp(big.NewInt(2)) >= 0 {
		errorFromAverage := new(big.Float).Sub(w.weightedMean, new(big.Float).SetInt(value))
		w.errorSquared.Add(w.errorSquared,
			new(big.Float).Mul(errorFromAverage, errorFromAverage))
	}

	// For the weightedMean
	deltaWeightedMean := new(big.Float).Sub(valueFloat, w.weightedMean)
	w.setWeightedMean(value)
	delta2WeightedMean := new(big.Float).Sub(valueFloat, w.weightedMean)
	w.sumDeltaWeightedMean.Add(w.sumDeltaWeightedMean, new(big.Float).Mul(deltaWeightedMean, delta2WeightedMean))

	// For the mean
	deltaMean := new(big.Float).Sub(valueFloat, w.mean)
	w.mean.Add(w.mean, new(big.Float).Quo(deltaMean, countFloat))
	delta2Mean := new(big.Float).Sub(valueFloat, w.mean)
	w.sumDeltaMean.Add(w.sumDeltaMean, new(big.Float).Mul(deltaMean, delta2Mean))

	// For the CPL
	w.cplSum += keySpace - int(bigmath.Log10(new(big.Int).Set(value))/bigmath.Log10(big.NewInt(2)))
}

func (w *WelfordAverage) setWeightedMean(value *big.Int) {
	if w.count.Int64() == 1 {
		w.weightedMean = new(big.Float).SetInt(value)
		return
	}

	// st+1 = (1 - alpha) * value + alpha * st
	leftFactor := big.NewFloat(1.0 - alpha)
	left := new(big.Float).Mul(leftFactor, w.weightedMean)

	rightFactor := big.NewFloat(alpha)
	right := new(big.Float).Mul(rightFactor, new(big.Float).SetInt(value))

	w.weightedMean = new(big.Float).Add(left, right)
}

func (w *WelfordAverage) GetErrorSquaredAverage() *big.Int {
	if w.count.Cmp(big.NewInt(1)) <= 0 {
		return big.NewInt(0)
	}

	countLessOne := new(big.Float).Sub(new(big.Float).SetInt(w.count), big.NewFloat(1))
	intValue, _ := new(big.Float).Quo(w.errorSquared, countLessOne).Int(new(big.Int))

	return intValue
}
