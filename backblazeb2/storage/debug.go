package storage

import (
	"fmt"
	"os"
	"strings"
	"time"
)

// debugf writes diagnostics to stderr only when PLAKAR_BACKBLAZEB2_DEBUG is
// enabled (1/true/yes/on).
func debugf(format string, args ...any) {
	v := strings.ToLower(strings.TrimSpace(os.Getenv("PLAKAR_BACKBLAZEB2_DEBUG")))
	if v != "1" && v != "true" && v != "yes" && v != "on" {
		return
	}
	_, _ = fmt.Fprintf(os.Stderr, "[%s] backblazeb2: "+format+"\n", append([]any{time.Now().Format(time.RFC3339)}, args...)...)
}
