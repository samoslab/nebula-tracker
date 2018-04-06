package sendmail

import (
	"fmt"
	"testing"
	"time"
)

func Test(t *testing.T) {
	// Send("test@email.com", "test-from-go", "this is a test email.")
	email := "test@email.com"
	randomCode := "test--random--code"
	Send(email, "Nebula Client Register Contact Email Verify Code", fmt.Sprintf("verify code is %s, sent at %s",
		randomCode, time.Now().UTC().Format("2006-01-02 15:04:05 UTC")))
}
