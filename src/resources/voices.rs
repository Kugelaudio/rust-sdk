//! Voice listing and management (`/v1/voices`).

use std::path::Path;

use reqwest::multipart::{Form, Part};
use reqwest::Method;
use serde::Serialize;

use crate::client::KugelAudio;
use crate::error::{Error, Result};
use crate::models::{GeneratedSample, VoiceDetail, VoiceId, VoiceListResponse, VoiceReference};

/// Filters and paging for [`VoicesResource::list`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct VoiceListOptions {
    /// Language filter (`de` matches every German accent, `de-AT` only Austrian).
    pub language: Option<String>,
    /// Include public catalog voices (default true).
    pub include_public: bool,
    /// Page size, 1-100 (default 20).
    pub limit: u32,
    /// Page offset (default 0).
    pub offset: u32,
}

impl Default for VoiceListOptions {
    fn default() -> Self {
        Self {
            language: None,
            include_public: true,
            limit: 20,
            offset: 0,
        }
    }
}

impl VoiceListOptions {
    /// Filter by language.
    pub fn language(mut self, language: impl Into<String>) -> Self {
        self.language = Some(language.into());
        self
    }

    /// Include or exclude public catalog voices.
    pub fn include_public(mut self, include_public: bool) -> Self {
        self.include_public = include_public;
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
}

/// An audio file to upload as voice reference.
#[derive(Clone, PartialEq, Eq)]
pub struct ReferenceFile {
    /// File name sent with the upload.
    pub filename: String,
    /// File contents.
    pub data: Vec<u8>,
}

impl std::fmt::Debug for ReferenceFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReferenceFile")
            .field("filename", &self.filename)
            .field("bytes", &self.data.len())
            .finish()
    }
}

impl ReferenceFile {
    /// Wrap bytes already in memory.
    pub fn new(filename: impl Into<String>, data: impl Into<Vec<u8>>) -> Self {
        Self {
            filename: filename.into(),
            data: data.into(),
        }
    }

    /// Read a file from disk.
    pub fn from_path(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let data = std::fs::read(path).map_err(|e| {
            Error::validation(format!(
                "Cannot read reference file {}: {e}",
                path.display()
            ))
            .with_source(e)
        })?;
        let filename = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| "reference.wav".to_string());
        Ok(Self { filename, data })
    }

    fn part(&self) -> Result<Part> {
        Part::bytes(self.data.clone())
            .file_name(self.filename.clone())
            .mime_str("audio/wav")
            .map_err(|e| Error::validation(e.to_string()))
    }
}

/// A new voice for [`VoicesResource::create`].
#[derive(Debug, Clone, PartialEq, Serialize)]
#[non_exhaustive]
pub struct VoiceCreate {
    /// Voice name.
    pub name: String,
    /// Voice sex (`male`, `female`).
    pub sex: String,
    /// Description.
    pub description: String,
    /// Category (default `conversational`).
    pub category: String,
    /// Age (`young`, `middle_age`, `old`; default `middle_age`).
    pub age: String,
    /// Quality (`low`, `mid`, `high`; default `mid`).
    pub quality: String,
    /// Language codes (default `["en"]`).
    pub supported_languages: Vec<String>,
    /// Whether the voice should be public.
    pub is_public: bool,
    /// Text used for automatic sample generation.
    pub sample_text: String,
    /// Your confirmation that you are the speaker of the recordings or have
    /// the speaker's permission to create a synthetic voice from them. The API
    /// rejects the request with `SPEAKER_CONSENT_REQUIRED` unless it is true,
    /// and records the confirmation with your account.
    pub speaker_consent: bool,
    /// Reference recordings uploaded with the voice.
    #[serde(skip)]
    pub reference_files: Vec<ReferenceFile>,
}

impl VoiceCreate {
    /// A voice named `name` of sex `sex`; `speaker_consent` is required.
    pub fn new(name: impl Into<String>, sex: impl Into<String>, speaker_consent: bool) -> Self {
        Self {
            name: name.into(),
            sex: sex.into(),
            description: String::new(),
            category: "conversational".to_string(),
            age: "middle_age".to_string(),
            quality: "mid".to_string(),
            supported_languages: vec!["en".to_string()],
            is_public: false,
            sample_text: String::new(),
            speaker_consent,
            reference_files: Vec::new(),
        }
    }

    /// Set the description.
    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = description.into();
        self
    }

    /// Set the category.
    pub fn category(mut self, category: impl Into<String>) -> Self {
        self.category = category.into();
        self
    }

    /// Set the age.
    pub fn age(mut self, age: impl Into<String>) -> Self {
        self.age = age.into();
        self
    }

    /// Set the quality.
    pub fn quality(mut self, quality: impl Into<String>) -> Self {
        self.quality = quality.into();
        self
    }

    /// Set the supported languages.
    pub fn supported_languages(mut self, languages: Vec<String>) -> Self {
        self.supported_languages = languages;
        self
    }

    /// Make the voice public.
    pub fn is_public(mut self, is_public: bool) -> Self {
        self.is_public = is_public;
        self
    }

    /// Set the sample text.
    pub fn sample_text(mut self, sample_text: impl Into<String>) -> Self {
        self.sample_text = sample_text.into();
        self
    }

    /// Add a reference recording.
    pub fn reference_file(mut self, file: ReferenceFile) -> Self {
        self.reference_files.push(file);
        self
    }
}

/// Fields to change with [`VoicesResource::update`]; `None` leaves a field unchanged.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
#[non_exhaustive]
pub struct VoiceUpdate {
    /// New name.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// New description.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// New category.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub category: Option<String>,
    /// New age.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub age: Option<String>,
    /// New sex.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sex: Option<String>,
    /// New quality.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quality: Option<String>,
    /// New language codes.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub supported_languages: Option<Vec<String>>,
    /// New public visibility.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_public: Option<bool>,
    /// New sample text.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sample_text: Option<String>,
}

impl VoiceUpdate {
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

    /// Change the category.
    pub fn category(mut self, value: impl Into<String>) -> Self {
        self.category = Some(value.into());
        self
    }

    /// Change the age.
    pub fn age(mut self, value: impl Into<String>) -> Self {
        self.age = Some(value.into());
        self
    }

    /// Change the sex.
    pub fn sex(mut self, value: impl Into<String>) -> Self {
        self.sex = Some(value.into());
        self
    }

    /// Change the quality.
    pub fn quality(mut self, value: impl Into<String>) -> Self {
        self.quality = Some(value.into());
        self
    }

    /// Change the language codes.
    pub fn supported_languages(mut self, value: Vec<String>) -> Self {
        self.supported_languages = Some(value);
        self
    }

    /// Change public visibility.
    pub fn is_public(mut self, value: bool) -> Self {
        self.is_public = Some(value);
        self
    }

    /// Change the sample text.
    pub fn sample_text(mut self, value: impl Into<String>) -> Self {
        self.sample_text = Some(value.into());
        self
    }
}

/// Voices, from [`KugelAudio::voices`].
#[derive(Debug, Clone, Copy)]
pub struct VoicesResource<'a> {
    client: &'a KugelAudio,
}

impl<'a> VoicesResource<'a> {
    pub(crate) fn new(client: &'a KugelAudio) -> Self {
        Self { client }
    }

    /// List voices (`GET /v1/voices`).
    pub async fn list(&self, options: &VoiceListOptions) -> Result<VoiceListResponse> {
        let mut query = vec![
            ("limit", options.limit.to_string()),
            ("offset", options.offset.to_string()),
        ];
        if let Some(language) = options.language.as_ref().filter(|l| !l.is_empty()) {
            query.push(("language", language.clone()));
        }
        if options.include_public {
            query.push(("include_public", "true".to_string()));
        }
        self.client
            .inner
            .json(Method::GET, "/v1/voices", &query, None)
            .await
    }

    /// One voice by id or handle (`GET /v1/voices/{voice_id}`).
    pub async fn get(&self, voice_id: impl Into<VoiceId>) -> Result<VoiceDetail> {
        let path = format!("/v1/voices/{}", voice_id.into());
        self.client.inner.json(Method::GET, &path, &[], None).await
    }

    /// Create a voice (`POST /v1/voices`, multipart metadata plus reference files).
    pub async fn create(&self, voice: &VoiceCreate) -> Result<VoiceDetail> {
        let metadata =
            serde_json::to_string(voice).map_err(|e| Error::validation(e.to_string()))?;
        let mut form = Form::new().part(
            "metadata",
            Part::text(metadata)
                .mime_str("application/json")
                .map_err(|e| Error::validation(e.to_string()))?,
        );
        for file in &voice.reference_files {
            form = form.part("files", file.part()?);
        }
        self.client
            .inner
            .multipart(Method::POST, "/v1/voices", form)
            .await
    }

    /// Update a voice (`PATCH /v1/voices/{voice_id}`); only set fields change.
    pub async fn update(
        &self,
        voice_id: impl Into<VoiceId>,
        update: &VoiceUpdate,
    ) -> Result<VoiceDetail> {
        let path = format!("/v1/voices/{}", voice_id.into());
        let body = serde_json::to_value(update).map_err(|e| Error::validation(e.to_string()))?;
        self.client
            .inner
            .json(Method::PATCH, &path, &[], Some(&body))
            .await
    }

    /// Delete a voice (`DELETE /v1/voices/{voice_id}`).
    pub async fn delete(&self, voice_id: impl Into<VoiceId>) -> Result<()> {
        let path = format!("/v1/voices/{}", voice_id.into());
        self.client.inner.empty(Method::DELETE, &path, &[]).await
    }

    /// The reference recordings of a voice (`GET /v1/voices/{voice_id}/references`).
    pub async fn list_references(
        &self,
        voice_id: impl Into<VoiceId>,
    ) -> Result<Vec<VoiceReference>> {
        let path = format!("/v1/voices/{}/references", voice_id.into());
        self.client.inner.json(Method::GET, &path, &[], None).await
    }

    /// Upload a reference recording (`POST /v1/voices/{voice_id}/references`).
    ///
    /// `speaker_consent` confirms you are the speaker or have the speaker's
    /// permission; the API rejects the upload unless it is true.
    pub async fn add_reference(
        &self,
        voice_id: impl Into<VoiceId>,
        file: &ReferenceFile,
        speaker_consent: bool,
        reference_text: Option<&str>,
    ) -> Result<VoiceReference> {
        let path = format!("/v1/voices/{}/references", voice_id.into());
        let mut form = Form::new().part("file", file.part()?).text(
            "speaker_consent",
            if speaker_consent { "true" } else { "false" },
        );
        if let Some(text) = reference_text.filter(|t| !t.is_empty()) {
            form = form.text("reference_text", text.to_string());
        }
        self.client.inner.multipart(Method::POST, &path, form).await
    }

    /// Delete a reference recording.
    pub async fn delete_reference(
        &self,
        voice_id: impl Into<VoiceId>,
        reference_id: i64,
    ) -> Result<()> {
        let path = format!("/v1/voices/{}/references/{reference_id}", voice_id.into());
        self.client.inner.empty(Method::DELETE, &path, &[]).await
    }

    /// Request publication: the voice becomes public once an admin verified it.
    pub async fn publish(&self, voice_id: impl Into<VoiceId>) -> Result<VoiceDetail> {
        let path = format!("/v1/voices/{}/publish", voice_id.into());
        self.client.inner.json(Method::POST, &path, &[], None).await
    }

    /// Generate the voice's preview sample from its sample text. The voice
    /// needs at least one reference.
    pub async fn generate_sample(&self, voice_id: impl Into<VoiceId>) -> Result<GeneratedSample> {
        let path = format!("/v1/voices/{}/generate-sample", voice_id.into());
        self.client.inner.json(Method::POST, &path, &[], None).await
    }
}
