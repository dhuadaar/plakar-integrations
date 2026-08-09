package main

import (
	"os"

	sdk "github.com/PlakarKorp/go-kloset-sdk"
	"github.com/PlakarKorp/integration-backblaze_s3/storage"
)

// main is intentionally tiny: Plakar launches this binary as a subprocess,
// and the SDK bridges protocol requests to `storage.NewStore`.
func main() {
	sdk.EntrypointStorage(os.Args, storage.NewStore)
}
