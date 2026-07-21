## Downloads

**Do not use** GitHub’s automatic `Source code (zip)` or `Source code (tar.gz)` links for this release. Those archives contain the entire repository, not just Mongosync Insights.

Use the release assets listed below instead.

| Platform | Download |
|----------|----------|
| macOS Apple Silicon | `mongosync-insights-<version>-macos-arm64` |
| Ubuntu amd64 | `mongosync-insights_<version>-1.ubuntu_amd64.deb` |
| Amazon Linux x86_64 | `mongosync-insights-<version>-1.amzn.x86_64.rpm` |
| RHEL 8 family | `mongosync-insights-<version>-1.el8.x86_64.rpm` |
| RHEL 9 family | `mongosync-insights-<version>-1.el9.x86_64.rpm` |
| MI source archive (tar.gz) | `mongosync-insights-<version>-source.tar.gz` |
| MI source archive (zip) | `mongosync-insights-<version>-source.zip` |

Release version: `<version>`.

### Quick start

* macOS: `chmod +x mongosync-insights-<version>-macos-arm64 && ./mongosync-insights-<version>-macos-arm64`
  * **First run:** If macOS blocks the app, right-click the file → **Open**, or run `xattr -cr ./mongosync-insights-<version>-macos-arm64` before launching. This is expected for unsigned binaries downloaded from GitHub.
* Ubuntu: `sudo apt install ./mongosync-insights_<version>-1.ubuntu_amd64.deb`
* Amazon Linux / RHEL: `sudo dnf install ./mongosync-insights-<version>-1.*.rpm`
* Source: extract the archive and use the `mongosync_insights/` folder

Full installation, upgrade, and build-from-source instructions are in [PACKAGING.md](https://github.com/mongodb/support-tools/blob/master/migration/mongosync_insights/PACKAGING.md).