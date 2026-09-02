package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func writeTokenFile(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "tokens.txt")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	return path
}

func TestLoadAuthTokenFileMergesTokensSkippingBlanksAndComments(t *testing.T) {
	path := writeTokenFile(t, "tok-a\n\n# a comment\ntok-b\n   \n#tok-c\n")
	cfg := Default()
	cfg.Auth.Tokens = []string{"pre-existing"}
	cfg.Auth.TokenFile = path

	require.NoError(t, cfg.loadAuthTokenFile())
	require.Equal(t, []string{"pre-existing", "tok-a", "tok-b"}, cfg.Auth.Tokens)
}

func TestLoadAuthTokenFileNoopWhenUnset(t *testing.T) {
	cfg := Default()
	require.NoError(t, cfg.loadAuthTokenFile())
	require.Empty(t, cfg.Auth.Tokens)
}

func TestLoadAuthTokenFileMissingFileErrors(t *testing.T) {
	cfg := Default()
	cfg.Auth.TokenFile = "/does/not/exist.txt"
	err := cfg.loadAuthTokenFile()
	require.Error(t, err)
	require.Contains(t, err.Error(), "read auth token-file")
}

// TestLoadThroughEnvTokenFileWiresIntoLoad pins that Config.Load itself calls
// loadAuthTokenFile, merging the file's tokens with PULSE_AUTH_TOKENS.
func TestLoadThroughEnvTokenFileWiresIntoLoad(t *testing.T) {
	path := writeTokenFile(t, "file-token\n")
	clearEnv(t)
	t.Setenv("PULSE_AUTH_TOKENS", "env-token")
	t.Setenv("PULSE_AUTH_TOKEN_FILE", path)

	cfg, err := Load("")
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"env-token", "file-token"}, cfg.Auth.Tokens)
}
