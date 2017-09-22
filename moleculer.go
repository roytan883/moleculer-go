package moleculer

import (
	log "github.com/Sirupsen/logrus"
)

func init() {
	//	// Log as JSON instead of the default ASCII formatter.
	log.SetFormatter(&log.TextFormatter{
		FullTimestamp: true,
		//		TimestampFormat:time.RFC3339Nano,
		TimestampFormat: "2006-01-02T15:04:05.000000000",
	})
	//
	//	// Output to stderr instead of stdout, could also be a file.
	//	log.SetOutput(os.Stderr)
	//
	//	// Only log the warning severity or above.
	//	log.SetLevel(log.WarnLevel)
}

//Create create a moleculer object
func Create() {

}

// Fn1 : just a test function
func Fn1(a int) int {
	log.Printf("Fn1 a = [%d]\n", a)
	return a + 1
}
