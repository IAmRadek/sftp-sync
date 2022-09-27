// streaming-read-benchmark benchmarks the peformance of reading
// from /dev/zero on the server to /dev/null on the client via io.Copy.
package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/caarlos0/env/v6"
	"github.com/pkg/sftp"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"golang.org/x/crypto/ssh"
)

func main() {
	run()
}

func run() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	config := struct {
		Host        string `env:"HOST,required"`
		Port        int    `env:"PORT" envDefault:"21"`
		Username    string `env:"USERNAME,required"`
		Password    string `env:"PASSWORD,unset"`
		PacketSize  int    `env:"PACKET_SIZE" envDefault:"32768"`
		Source      string `env:"SOURCE,required"`
		Destination string `env:"DESTINATION,required"`
	}{}
	if err := env.Parse(&config); err != nil {
		log.Fatal().Err(err).Msg("Failed to parse environment variables")
	}

	var auths []ssh.AuthMethod
	if config.Password != "" {
		auths = append(auths, ssh.Password(config.Password))
	}

	sshCfg := ssh.ClientConfig{
		User:            config.Username,
		Auth:            auths,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	}
	addr := fmt.Sprintf("%s:%d", config.Host, config.Port)
	conn, err := ssh.Dial("tcp", addr, &sshCfg)
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to dial")
	}
	defer func(conn *ssh.Client) {
		closeErr := conn.Close()
		if closeErr != nil {
			log.Error().Err(closeErr).Msg("Failed to close connection")
		}
	}(conn)

	c, err := sftp.NewClient(conn, sftp.MaxPacket(config.PacketSize))
	if err != nil {
		log.Fatal().Err(err).Msg("Failed to create sftp client")
	}
	defer func(c *sftp.Client) {
		closeErr := c.Close()
		if closeErr != nil {
			log.Error().Err(closeErr).Msg("Failed to close sftp client")
		}
	}(c)

	log.Info().Msg("Starting to copy files")

	toRetry := make(chan [2]string, 100)
	semaphore := make(chan struct{}, 10)
	wg := sync.WaitGroup{}

	walker := c.Walk(config.Source)
	for walker.Step() {
		if walker.Err() != nil {
			log.Error().Err(walker.Err()).Msg("Failed to walk")
			continue
		}
		src := walker.Path()
		dst := filepath.Join(config.Destination, walker.Path()[len(config.Source):])
		if walker.Stat().IsDir() {
			createDir(dst)
			continue
		}

		semaphore <- struct{}{}
		wg.Add(1)
		go func(src, dst string) {
			defer func() { <-semaphore }()
			defer wg.Done()

			err := downloadFile(c, src, dst)
			if err != nil {
				log.Error().Err(err).Msgf("Failed to download file %s", src)
				select {
				case toRetry <- [2]string{src, dst}:
				default:
					log.Error().Msg("Failed to add to retry channel")
				}
			}
		}(src, dst)
	}
	wg.Wait()
	close(toRetry)

	log.Info().Msg("Finished copying files")

	log.Info().Msg("Starting to retry failed downloads")
	for toRetryFile := range toRetry {
		wg.Add(1)
		go func(src, dst string) {
			defer wg.Done()

			err := downloadFile(c, src, dst)
			if err != nil {
				log.Error().Err(err).Msgf("Failed to download file %s", src)
			}
		}(toRetryFile[0], toRetryFile[1])
	}

	wg.Wait()
	log.Info().Msg("Finished retrying failed downloads")
}
func downloadFile(c *sftp.Client, src, dest string) error {
	srcFile, err := c.Open(src)
	if err != nil {
		return err
	}

	dstFile, err := os.Create(dest)
	if err != nil {
		return err
	}

	_, err = io.Copy(dstFile, srcFile)
	if err != nil {
		return err
	}

	closeErr := dstFile.Close()
	if closeErr != nil {
		log.Error().Err(closeErr).Msgf("Failed to close destination file %s", dest)

		return nil
	}

	closeErr = srcFile.Close()
	if closeErr != nil {
		log.Error().Err(closeErr).Msgf("Failed to close source file %s", src)
		return nil
	}
	return nil
}

func createDir(dir string) {
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		log.Error().Err(err).Msgf("Failed to create directory %s", dir)
	}
}
