package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

const logBottlePathWithoutHome = "Library/Application Support/CrossOver/Bottles/Steam Library/drive_c/users/crossover/AppData/Local/Warframe/EE.log"

func isValidPath(path string) bool {
	_, err := os.Stat(path)
	return err != nil
}

func buildPath() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("Failed to resolve user home directory: %s", err)
	}
	return filepath.Clean(strconv.Quote(filepath.Join(home, logBottlePathWithoutHome))), nil
}

func main() {
	path, err := buildPath()
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(isValidPath(path))
}
