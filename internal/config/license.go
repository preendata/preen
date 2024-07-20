package config

import (
	"context"
	"fmt"

	"github.com/denisbrodbeck/machineid"
	"github.com/keygen-sh/keygen-go/v3"
)

func validateLicenseKey(licenseKey string) error {
	keygen.Account = "80e586b8-dfde-4eda-a8bb-ba222a630dec"
	keygen.Product = "0c8ee7ec-09d8-4925-89c6-5e1093942800"
	keygen.PublicKey = "a266c7bc3c90c431d320d64f2da0fb5f057d05a4f9bc3c8b46090c9ab168939c"
	keygen.LicenseKey = licenseKey

	fingerprint, err := machineid.ProtectedID(keygen.Product)
	if err != nil {
		return err
	}

	ctx := context.Background()

	// Validate the license for the current fingerprint
	license, err := keygen.Validate(ctx, fingerprint)

	switch {
	case err == keygen.ErrLicenseNotActivated:
		// Activate the current fingerprint
		_, err := license.Activate(ctx, fingerprint)
		switch {
		case err == keygen.ErrMachineLimitExceeded:
			return fmt.Errorf("machine limit has been exceeded for current license key")
		case err != nil:
			return fmt.Errorf("machine license activation failed")
		}
	case err == keygen.ErrLicenseExpired:
		return fmt.Errorf("hyphadb license is expired")
	case err == keygen.ErrLicenseSuspended:
		return fmt.Errorf("hyphadb license is suspended")
	case err != nil:
		return fmt.Errorf("hyphadb license is invalid %v", err)
	}

	return nil
}
