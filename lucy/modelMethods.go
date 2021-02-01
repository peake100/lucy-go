package lucy

// Eq returns true if two *StageID values are equal.
func (x *StageID) Eq(other *StageID) bool {
	return x.StageIndex == other.StageIndex &&
		x.JobId.MustGoogle() == other.JobId.MustGoogle()
}
