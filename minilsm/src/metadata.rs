pub struct ManifestUpdate {
    added_files: Vec<String>,
    removed_files: Vec<String>
}

impl ManifestUpdate {
    pub fn new(added_files: Vec<String>, removed_files: Vec<String>) -> Self {
        ManifestUpdate { added_files, removed_files }
    }
}

impl Default for ManifestUpdate {
    fn default() -> Self {
        ManifestUpdate::new(Vec::new(), Vec::new())
    }
}
