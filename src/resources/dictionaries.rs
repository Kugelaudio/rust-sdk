//! Per-project pronunciation dictionaries (`/v1/dictionaries`).
//!
//! The server invalidates its cache after every change, so the next synthesis
//! request uses it immediately. `project_id` is needed only with an
//! organization-wide key; a project key uses its own project.

use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::client::KugelAudio;
use crate::error::{Error, Result};
use crate::models::{
    BulkReplaceResult, Dictionary, DictionaryEntry, DictionaryEntryInput, DictionaryEntryList,
};

fn project_query(project_id: Option<i64>) -> Vec<(&'static str, String)> {
    project_id
        .map(|id| vec![("project_id", id.to_string())])
        .unwrap_or_default()
}

fn to_body(value: &impl Serialize) -> Result<serde_json::Value> {
    serde_json::to_value(value).map_err(|e| Error::validation(e.to_string()))
}

/// A new dictionary for [`DictionariesResource::create`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct DictionaryCreate {
    /// Name, unique within the project.
    pub name: String,
    /// Description.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Language the dictionary applies to; `None` for every language.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub language: Option<String>,
}

impl DictionaryCreate {
    /// A dictionary named `name`.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            description: None,
            language: None,
        }
    }

    /// Set the description.
    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Restrict the dictionary to one language.
    pub fn language(mut self, language: impl Into<String>) -> Self {
        self.language = Some(language.into());
        self
    }
}

/// Fields to change with [`DictionariesResource::update`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct DictionaryUpdate {
    /// New name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// New description.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// New language.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub language: Option<String>,
    /// Activate or deactivate the dictionary.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_active: Option<bool>,
}

impl DictionaryUpdate {
    /// An update that changes nothing yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// Change the name.
    pub fn name(mut self, value: impl Into<String>) -> Self {
        self.name = Some(value.into());
        self
    }

    /// Change the description.
    pub fn description(mut self, value: impl Into<String>) -> Self {
        self.description = Some(value.into());
        self
    }

    /// Change the language.
    pub fn language(mut self, value: impl Into<String>) -> Self {
        self.language = Some(value.into());
        self
    }

    /// Activate or deactivate.
    pub fn is_active(mut self, value: bool) -> Self {
        self.is_active = Some(value);
        self
    }
}

/// Paging and search for [`DictionaryEntriesResource::list`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct EntryListOptions {
    /// Case-insensitive substring filter on `word`.
    pub search: Option<String>,
    /// Page size, 1-500 (default 100).
    pub limit: u32,
    /// Page offset.
    pub offset: u32,
    /// Project, for organization-wide keys.
    pub project_id: Option<i64>,
}

impl Default for EntryListOptions {
    fn default() -> Self {
        Self {
            search: None,
            limit: 100,
            offset: 0,
            project_id: None,
        }
    }
}

impl EntryListOptions {
    /// Filter by a substring of `word`.
    pub fn search(mut self, search: impl Into<String>) -> Self {
        self.search = Some(search.into());
        self
    }

    /// Set the page size.
    pub fn limit(mut self, limit: u32) -> Self {
        self.limit = limit;
        self
    }

    /// Set the page offset.
    pub fn offset(mut self, offset: u32) -> Self {
        self.offset = offset;
        self
    }

    /// Set the project.
    pub fn project_id(mut self, project_id: i64) -> Self {
        self.project_id = Some(project_id);
        self
    }
}

/// A new entry for [`DictionaryEntriesResource::add`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct NewDictionaryEntry {
    /// The word to replace.
    pub word: String,
    /// What to say instead.
    pub replacement: String,
    /// Optional IPA pronunciation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ipa: Option<String>,
    /// Case-sensitive matching (default false).
    pub case_sensitive: bool,
}

impl NewDictionaryEntry {
    /// An entry replacing `word` with `replacement`.
    pub fn new(word: impl Into<String>, replacement: impl Into<String>) -> Self {
        Self {
            word: word.into(),
            replacement: replacement.into(),
            ipa: None,
            case_sensitive: false,
        }
    }

    /// Set the IPA pronunciation.
    pub fn ipa(mut self, ipa: impl Into<String>) -> Self {
        self.ipa = Some(ipa.into());
        self
    }

    /// Match case-sensitively.
    pub fn case_sensitive(mut self, case_sensitive: bool) -> Self {
        self.case_sensitive = case_sensitive;
        self
    }
}

/// Fields to change with [`DictionaryEntriesResource::update`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct DictionaryEntryUpdate {
    /// New word.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub word: Option<String>,
    /// New replacement.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replacement: Option<String>,
    /// New IPA pronunciation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ipa: Option<String>,
    /// New case sensitivity.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub case_sensitive: Option<bool>,
}

impl DictionaryEntryUpdate {
    /// An update that changes nothing yet.
    pub fn new() -> Self {
        Self::default()
    }

    /// Change the word.
    pub fn word(mut self, value: impl Into<String>) -> Self {
        self.word = Some(value.into());
        self
    }

    /// Change the replacement.
    pub fn replacement(mut self, value: impl Into<String>) -> Self {
        self.replacement = Some(value.into());
        self
    }

    /// Change the IPA pronunciation.
    pub fn ipa(mut self, value: impl Into<String>) -> Self {
        self.ipa = Some(value.into());
        self
    }

    /// Change case sensitivity.
    pub fn case_sensitive(mut self, value: bool) -> Self {
        self.case_sensitive = Some(value);
        self
    }
}

#[derive(Deserialize)]
struct DictionaryList {
    #[serde(default)]
    dictionaries: Vec<Dictionary>,
}

/// Dictionaries, from [`KugelAudio::dictionaries`].
#[derive(Debug, Clone, Copy)]
pub struct DictionariesResource<'a> {
    client: &'a KugelAudio,
}

impl<'a> DictionariesResource<'a> {
    pub(crate) fn new(client: &'a KugelAudio) -> Self {
        Self { client }
    }

    /// The entries of one dictionary.
    pub fn entries(&self) -> DictionaryEntriesResource<'a> {
        DictionaryEntriesResource {
            client: self.client,
        }
    }

    /// Every dictionary of the project.
    pub async fn list(&self, project_id: Option<i64>) -> Result<Vec<Dictionary>> {
        let list: DictionaryList = self
            .client
            .inner
            .json(
                Method::GET,
                "/v1/dictionaries",
                &project_query(project_id),
                None,
            )
            .await?;
        Ok(list.dictionaries)
    }

    /// Create a dictionary.
    pub async fn create(
        &self,
        dictionary: &DictionaryCreate,
        project_id: Option<i64>,
    ) -> Result<Dictionary> {
        let body = to_body(dictionary)?;
        self.client
            .inner
            .json(
                Method::POST,
                "/v1/dictionaries",
                &project_query(project_id),
                Some(&body),
            )
            .await
    }

    /// One dictionary.
    pub async fn get(&self, dictionary_id: i64, project_id: Option<i64>) -> Result<Dictionary> {
        let path = format!("/v1/dictionaries/{dictionary_id}");
        self.client
            .inner
            .json(Method::GET, &path, &project_query(project_id), None)
            .await
    }

    /// Change name, description, language or `is_active`.
    pub async fn update(
        &self,
        dictionary_id: i64,
        update: &DictionaryUpdate,
        project_id: Option<i64>,
    ) -> Result<Dictionary> {
        let path = format!("/v1/dictionaries/{dictionary_id}");
        let body = to_body(update)?;
        self.client
            .inner
            .json(
                Method::PATCH,
                &path,
                &project_query(project_id),
                Some(&body),
            )
            .await
    }

    /// Delete a dictionary and its entries.
    pub async fn delete(&self, dictionary_id: i64, project_id: Option<i64>) -> Result<()> {
        let path = format!("/v1/dictionaries/{dictionary_id}");
        self.client
            .inner
            .empty(Method::DELETE, &path, &project_query(project_id))
            .await
    }
}

/// Dictionary entries, from [`DictionariesResource::entries`].
#[derive(Debug, Clone, Copy)]
pub struct DictionaryEntriesResource<'a> {
    client: &'a KugelAudio,
}

impl DictionaryEntriesResource<'_> {
    /// List entries, optionally filtered by a substring of `word`.
    pub async fn list(
        &self,
        dictionary_id: i64,
        options: &EntryListOptions,
    ) -> Result<DictionaryEntryList> {
        let path = format!("/v1/dictionaries/{dictionary_id}/entries");
        let mut query = vec![
            ("limit", options.limit.to_string()),
            ("offset", options.offset.to_string()),
        ];
        if let Some(search) = &options.search {
            query.push(("search", search.clone()));
        }
        query.extend(project_query(options.project_id));
        self.client
            .inner
            .json(Method::GET, &path, &query, None)
            .await
    }

    /// Add one entry.
    pub async fn add(
        &self,
        dictionary_id: i64,
        entry: &NewDictionaryEntry,
        project_id: Option<i64>,
    ) -> Result<DictionaryEntry> {
        let path = format!("/v1/dictionaries/{dictionary_id}/entries");
        let body = to_body(entry)?;
        self.client
            .inner
            .json(Method::POST, &path, &project_query(project_id), Some(&body))
            .await
    }

    /// Change an entry; only set fields change.
    pub async fn update(
        &self,
        dictionary_id: i64,
        entry_id: i64,
        update: &DictionaryEntryUpdate,
        project_id: Option<i64>,
    ) -> Result<DictionaryEntry> {
        let path = format!("/v1/dictionaries/{dictionary_id}/entries/{entry_id}");
        let body = to_body(update)?;
        self.client
            .inner
            .json(
                Method::PATCH,
                &path,
                &project_query(project_id),
                Some(&body),
            )
            .await
    }

    /// Delete an entry.
    pub async fn delete(
        &self,
        dictionary_id: i64,
        entry_id: i64,
        project_id: Option<i64>,
    ) -> Result<()> {
        let path = format!("/v1/dictionaries/{dictionary_id}/entries/{entry_id}");
        self.client
            .inner
            .empty(Method::DELETE, &path, &project_query(project_id))
            .await
    }

    /// Atomically replace every entry with `entries` (idempotent; entries
    /// whose word is not in the list are deleted).
    pub async fn replace_all(
        &self,
        dictionary_id: i64,
        entries: &[DictionaryEntryInput],
        project_id: Option<i64>,
    ) -> Result<BulkReplaceResult> {
        let path = format!("/v1/dictionaries/{dictionary_id}/entries");
        let body = json!({ "entries": entries });
        self.client
            .inner
            .json(Method::PUT, &path, &project_query(project_id), Some(&body))
            .await
    }
}
