package main

import (
	"errors"

	"io"

	"os"

	"strconv"
	"strings"

	"github.com/emersion/go-imap"
	"github.com/emersion/go-imap/client"
	"github.com/emersion/go-message"
	"github.com/emersion/go-message/mail"
	"github.com/okonma-violet/spec/logs/logger"
	"golang.org/x/text/encoding/charmap"
)

func checkMail(l logger.Logger, downloadspath string, suppliers []supplier) error {
	l.Debug("checkMail", "Connecting to server...")

	message.CharsetReader = func(charset string, input io.Reader) (io.Reader, error) {
		if charset == "windows-1251" {
			decoder := charmap.Windows1251.NewDecoder()
			r := decoder.Reader(input)
			return r, nil
		}
		return input, nil
	}
	// Connect to server
	c, err := client.DialTLS("imap.mail.ru:993", nil)
	if err != nil {
		return err
	}
	l.Debug("checkMail", "Connected")

	// Don't forget to logout
	defer c.Logout()

	// Login
	if err := c.Login("autodoc_price@mail.ru", "pVRqHJWdfwJsx7TLuVWu"); err != nil {
		return err
	}
	l.Debug("checkMail", "Logged in")

	// List mailboxes
	// mailboxes := make(chan *imap.MailboxInfo, 10)
	// done := make(chan error, 1)
	// go func() {
	// 	done <- c.List("", "*", mailboxes)
	// }()

	// log.Println("Mailboxes:")
	// for m := range mailboxes {
	// 	log.Println("* " + m.Name)
	// }

	// if err := <-done; err != nil {
	// 	log.Fatal(err)
	// }

	// Select INBOX
	mbox, err := c.Select("Я - Прайсы", false)
	if err != nil {
		return err
	}
	l.Debug("checkMail", "Flags for INBOX: ["+strings.Join(mbox.Flags, ", ")+"]")

	// Get the last 4 messages
	data, err := os.ReadFile("lastmessage")
	if err != nil {
		return err
	}
	i, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return err
	}
	from := uint32(i + 1)
	to := mbox.Messages
	l.Debug("checkMail", "Total messages: "+strconv.FormatUint(uint64(to), 10)+", load from "+strconv.FormatUint(uint64(from), 10))
	seqset := new(imap.SeqSet)
	seqset.AddRange(from, to)

	messages := make(chan *imap.Message, 30)
	done := make(chan error, 1)
	var section imap.BodySectionName
	go func() {
		done <- c.Fetch(seqset, []imap.FetchItem{imap.FetchEnvelope, section.FetchItem()}, messages)
	}()
	var has_suitabled int
	for msg := range messages {
		//log.Println("* "+msg.Envelope.Subject, msg.Envelope.From[0].Address(), len(msg.Items), len(msg.Body))
		cur_sups := getSupsByMail(suppliers, msg.Envelope.From[0].Address())
		if len(cur_sups) == 0 {
			continue
		}
		r := msg.GetBody(&section)
		if r == nil {
			return errors.New("server didn't returned message body")
		}

		// Create a new mail reader
		mr, err := mail.CreateReader(r)
		if err != nil {
			return err
		}

		// Print some info about the message
		header := mr.Header
		if date, err := header.Date(); err == nil {
			l.Debug("checkMail", "Date: "+date.String())
		}
		if from, err := header.AddressList("From"); err == nil {
			var frs string
			for _, fr := range from {
				frs += " " + fr.String()
			}
			frs += "]"
			l.Debug("checkMail", "From: ["+frs)
		}
		// if to, err := header.AddressList("To"); err == nil {
		// 	log.Println("To:", to)
		// }
		if subject, err := header.Subject(); err == nil {
			l.Debug("checkMail", "Subject: "+subject)
		}

		// Process each message's part
		for {
			p, err := mr.NextPart()
			if err == io.EOF {
				break
			} else if err != nil {
				return err
			}

			switch h := p.Header.(type) {
			// case *mail.InlineHeader:
			// 	// This is the message's text (can be plain-text or HTML)
			// 	b, _ := ioutil.ReadAll(p.Body)
			// 	log.Println("Got text: %v", string(b))
			case *mail.AttachmentHeader:
				// This is an attachment
				filename, _ := h.Filename()
				l.Debug("checkMail", "Got attachment: "+filename)
				if !suitableFilename(cur_sups, filename) {
					l.Debug("checkMail", "Not suitable attachment: "+filename)
					continue
				}
				has_suitabled++
				// Create file with attachment name
				file, err := os.Create(downloadspath + filename)
				if err != nil {
					return err
				}
				// using io.Copy instead of io.ReadAll to avoid insufficient memory issues
				size, err := io.Copy(file, p.Body)
				if err != nil {
					file.Close()
					return err
				}
				file.Close()
				l.Debug("checkMail", "Saved "+strconv.FormatInt(size, 10)+" bytes into "+filename)
			}
			if err = os.WriteFile("lastmessage", []byte(strconv.Itoa(int(msg.SeqNum))), 0644); err != nil {
				return err
			}
		}
		if has_suitabled < 1 {
			l.Warning("checkMail", "No suitabled attachments in message from: "+msg.Envelope.From[0].Address())
		}

	}

	if err := <-done; err != nil {
		return err
	}

	l.Debug("checkMail", "Done!")
	return nil
}

func IsSupplierEmail(suppliers []supplier, email string) bool {
	email = strings.ToLower(email)
	for _, s := range suppliers {
		if s.Email == email {
			return true
		}
	}
	return false
}

func getSupsByMail(sups []supplier, email string) []*supplier {
	email = strings.ToLower(email)
	res := make([]*supplier, 0)
	for i := 0; i < len(sups); i++ {
		if sups[i].Email == email {

			res = append(res, &sups[i])
		}
	}
	return res
}

func suitableFilename(sups []*supplier, filename string) bool {
	filename = strings.ToLower(filename)
	for i := 0; i < len(sups); i++ {
		for k := 0; k < len(sups[i].MailFileNamePattern_Prefixes); k++ {
			if strings.HasPrefix(filename, sups[i].MailFileNamePattern_Prefixes[k]) && strings.HasSuffix(filename, sups[i].MailFileNamePattern_Suffixes[k]) {
				return true
			}
		}
	}
	return false
}
