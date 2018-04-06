package sendmail

import (
	"crypto/tls"
	"fmt"
	"nebula-tracker/config"
	"net/mail"
	"net/smtp"
)

func dial(addr string) (*tls.Conn, error) {
	return tls.Dial("tcp", addr, nil)
}

func composeMsg(from string, to string, subject string, body string) (message string) {
	// Setup headers
	headers := make(map[string]string)
	headers["From"] = from
	headers["To"] = to
	headers["Subject"] = subject
	// Setup message
	for k, v := range headers {
		message += fmt.Sprintf("%s: %s\r\n", k, v)
	}
	message += "\r\n" + body
	return
}

func Send(toAddr string, subject string, body string) (err error) {
	conf := config.GetTrackerConfig().Smtps
	// get SSL connection
	conn, err := dial(fmt.Sprintf("%s:%d", conf.Host, conf.Port))
	if err != nil {
		return
	}
	// create new SMTP client
	smtpClient, err := smtp.NewClient(conn, conf.Host)
	if err != nil {
		return
	}
	// Set up authentication information.
	auth := smtp.PlainAuth("", conf.Username, conf.Password, conf.Host)
	// auth the smtp client
	err = smtpClient.Auth(auth)
	if err != nil {
		return
	}
	// set To && From address, note that from address must be same as authorization user.
	from := mail.Address{Address: conf.Username}
	to := mail.Address{Address: toAddr}
	err = smtpClient.Mail(from.Address)
	if err != nil {
		return
	}
	err = smtpClient.Rcpt(to.Address)
	if err != nil {
		return
	}
	// Get the writer from SMTP client
	writer, err := smtpClient.Data()
	if err != nil {
		return
	}
	// compose message body
	message := composeMsg(from.String(), to.String(), subject, body)
	// write message to recp
	_, err = writer.Write([]byte(message))
	if err != nil {
		return
	}
	// close the writer
	err = writer.Close()
	if err != nil {
		return
	}
	// Quit sends the QUIT command and closes the connection to the server.
	smtpClient.Quit()
	return nil
}
