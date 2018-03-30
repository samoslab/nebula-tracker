package config

import (
	"bufio"
	"fmt"
	"os"
	"testing"
)

func TestGetTrackerConfig(t *testing.T) {
	writeConfigFile("")
	defer removeConfigFile()
	if GetTrackerConfig() == nil {
		t.Errorf("Failed. got nil")
	}
}

func removeConfigFile() {
	err := os.Remove(config_filename)
	if err != nil {
		fmt.Printf("%s\n", err)
	}
}
func writeConfigFile(content string) {
	outputFile, err := os.OpenFile(config_filename, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		fmt.Printf("%s\n", err)
		return
	}
	defer outputFile.Close()

	outputWriter := bufio.NewWriter(outputFile)
	outputWriter.WriteString(content)
	outputWriter.Flush()
}
