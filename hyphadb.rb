class HyphaDB < Formula
  desc "HyphaDB Description"
  homepage "https://github.com/hyphadb/hyphadb"
  url ".../mytool-darwin-amd64.tar.gz", :using => GitHubPrivateRepositoryReleaseDownloadStrategy
#   sha256 "<SHA256 CHECKSUM>"
  head "https://github.com/hyphadb/hyphadb.git"

  def install
    bin.install "hyphadb"
  end

  # Homebrew requires tests.
  test do
    assert_match "lol", "lol"
  end
end