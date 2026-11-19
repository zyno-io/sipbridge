package engine

import "testing"

func TestRedisApplicationInviteACKOutcomes(t *testing.T) {
	for _, test := range []struct {
		name string
		kind int
	}{{"not-written", 55}, {"unknown", 56}, {"non-2xx", 57}, {"remote-offer", 58}, {"deferred-cancel", 61}, {"cancel-success", 62}, {"ack-result-expires-first", 64}, {"overdue-m-no-transfer", 65}, {"m-expires-first", 66}, {"previous-non-2xx-replay", 67}, {"ack-late-written-during-bye", 68}, {"ack-unknown-terminal", 69}} {
		t.Run(test.name, func(t *testing.T) {
			testRedisUACQualificationInitialAdmission(t, false, false, false, true, false, test.kind)
		})
	}
}

func TestRedisApplicationInviteTimeout(t *testing.T) {
	t.Run("written", func(t *testing.T) {
		testRedisUACQualificationInitialAdmission(t, false, false, false, true, false, 72)
	})
	t.Run("uncertain-write", func(t *testing.T) {
		testRedisUACQualificationInitialAdmission(t, false, false, false, true, false, 73)
	})
}
