//! Typed objects returned by the REST endpoints: models, voices, dictionaries.

use std::fmt;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A voice, by numeric id or by handle (e.g. `"emma"`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum VoiceId {
    /// The numeric voice id.
    Id(i64),
    /// The voice handle.
    Handle(String),
}

impl fmt::Display for VoiceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            VoiceId::Id(id) => write!(f, "{id}"),
            VoiceId::Handle(handle) => f.write_str(handle),
        }
    }
}

impl From<i64> for VoiceId {
    fn from(id: i64) -> Self {
        VoiceId::Id(id)
    }
}

impl From<i32> for VoiceId {
    fn from(id: i32) -> Self {
        VoiceId::Id(i64::from(id))
    }
}

impl From<u32> for VoiceId {
    fn from(id: u32) -> Self {
        VoiceId::Id(i64::from(id))
    }
}

impl From<&str> for VoiceId {
    fn from(handle: &str) -> Self {
        VoiceId::Handle(handle.to_string())
    }
}

impl From<String> for VoiceId {
    fn from(handle: String) -> Self {
        VoiceId::Handle(handle)
    }
}

impl Serialize for VoiceId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            VoiceId::Id(id) => serializer.serialize_i64(*id),
            VoiceId::Handle(handle) => serializer.serialize_str(handle),
        }
    }
}

/// A TTS model from the catalog.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[non_exhaustive]
pub struct Model {
    /// Model id accepted in API calls, e.g. `kugel-3.5`.
    pub id: String,
    /// Human-readable name.
    pub name: String,
    /// Description, when the catalog has one.
    #[serde(default)]
    pub description: Option<String>,
    /// Parameter count label; `None` for current models.
    #[serde(default)]
    pub parameters: Option<String>,
    /// Maximum input text length in characters.
    #[serde(default)]
    pub max_input_length: Option<u32>,
    /// Native model sample rate in Hz.
    #[serde(default)]
    pub sample_rate: Option<u32>,
}

macro_rules! string_enum {
    ($(#[$doc:meta])* $name:ident { $($(#[$vdoc:meta])* $variant:ident => $wire:literal),* $(,)? }) => {
        $(#[$doc])*
        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        #[non_exhaustive]
        pub enum $name {
            $($(#[$vdoc])* $variant,)*
            /// A value this SDK version does not know, kept verbatim.
            Other(String),
        }

        impl $name {
            /// The wire value.
            pub fn as_str(&self) -> &str {
                match self {
                    $($name::$variant => $wire,)*
                    $name::Other(value) => value,
                }
            }
        }

        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                match value {
                    $($wire => $name::$variant,)*
                    other => $name::Other(other.to_string()),
                }
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.as_str())
            }
        }

        impl Serialize for $name {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.serialize_str(self.as_str())
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                let value = String::deserialize(deserializer)?;
                Ok($name::from(value.as_str()))
            }
        }
    };
}

string_enum!(
    /// Voice category. The API returns `cloned` for a voice with no category set.
    VoiceCategory {
        /// `premade`
        Premade => "premade",
        /// `cloned`
        Cloned => "cloned",
        /// `designed`
        Designed => "designed",
        /// `conversational`
        Conversational => "conversational",
        /// `narrative`
        Narrative => "narrative",
        /// `narrative_story`
        NarrativeStory => "narrative_story",
        /// `characters`
        Characters => "characters",
        /// `characters_animation`
        CharactersAnimation => "characters_animation",
        /// `social_media`
        SocialMedia => "social_media",
        /// `entertainment_tv`
        EntertainmentTv => "entertainment_tv",
        /// `advertisement`
        Advertisement => "advertisement",
        /// `informative_educational`
        InformativeEducational => "informative_educational",
    }
);

string_enum!(
    /// Voice sex.
    VoiceSex {
        /// `male`
        Male => "male",
        /// `female`
        Female => "female",
        /// `neutral`
        Neutral => "neutral",
    }
);

string_enum!(
    /// Voice age.
    VoiceAge {
        /// `young`
        Young => "young",
        /// `middle_aged`
        MiddleAged => "middle_aged",
        /// `middle_age` (alternative spelling the API also uses)
        MiddleAge => "middle_age",
        /// `old`
        Old => "old",
    }
);

/// A voice in a listing.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[non_exhaustive]
pub struct Voice {
    /// Numeric voice id.
    pub id: i64,
    /// Voice handle, when the voice has one.
    #[serde(default)]
    pub handle: Option<String>,
    /// Display name.
    pub name: String,
    /// Description.
    #[serde(default)]
    pub description: Option<String>,
    /// Category.
    #[serde(default)]
    pub category: Option<VoiceCategory>,
    /// Sex.
    #[serde(default)]
    pub sex: Option<VoiceSex>,
    /// Age.
    #[serde(default)]
    pub age: Option<VoiceAge>,
    /// Quality label (`low`, `mid`, `high`).
    #[serde(default)]
    pub quality: Option<String>,
    /// BCP-47 language tags the voice speaks.
    #[serde(default, deserialize_with = "null_as_default")]
    pub supported_languages: Vec<String>,
    /// Text used for the voice's sample.
    #[serde(default)]
    pub sample_text: Option<String>,
    /// Avatar image URL.
    #[serde(default)]
    pub avatar_url: Option<String>,
    /// Sample audio URL.
    #[serde(default)]
    pub sample_url: Option<String>,
    /// Whether the voice is public.
    #[serde(default, deserialize_with = "null_as_default")]
    pub is_public: bool,
    /// Whether the voice is verified.
    #[serde(default, deserialize_with = "null_as_default")]
    pub verified: bool,
}

/// One page of voices.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[non_exhaustive]
pub struct VoiceListResponse {
    /// The voices on this page.
    #[serde(default)]
    pub voices: Vec<Voice>,
    /// Total matching voices: your own voices plus the public catalog.
    pub total: u64,
    /// Page size.
    pub limit: u64,
    /// Page offset.
    pub offset: u64,
}

/// Full voice information returned by the voice management endpoints.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[non_exhaustive]
pub struct VoiceDetail {
    /// Numeric voice id.
    pub id: i64,
    /// Voice handle, when the voice has one.
    #[serde(default)]
    pub handle: Option<String>,
    /// Display name.
    pub name: String,
    /// Description.
    #[serde(default)]
    pub description: Option<String>,
    /// Generated description of the voice's character.
    #[serde(default)]
    pub generative_voice_description: Option<String>,
    /// BCP-47 language tags the voice speaks.
    #[serde(default, deserialize_with = "null_as_default")]
    pub supported_languages: Vec<String>,
    /// Category.
    #[serde(default)]
    pub category: Option<VoiceCategory>,
    /// Age.
    #[serde(default)]
    pub age: Option<VoiceAge>,
    /// Sex.
    #[serde(default)]
    pub sex: Option<VoiceSex>,
    /// Quality label.
    #[serde(default)]
    pub quality: Option<String>,
    /// Whether the voice is public.
    #[serde(default, deserialize_with = "null_as_default")]
    pub is_public: bool,
    /// Whether the voice is verified.
    #[serde(default, deserialize_with = "null_as_default")]
    pub verified: bool,
    /// Whether publication is waiting for verification.
    #[serde(default, deserialize_with = "null_as_default")]
    pub pending_verification: bool,
    /// Sample audio URL.
    #[serde(default)]
    pub sample_url: Option<String>,
    /// Avatar image URL.
    #[serde(default)]
    pub avatar_url: Option<String>,
    /// Text used for automatic sample generation.
    #[serde(default)]
    pub sample_text: Option<String>,
}

/// Reference audio of a voice.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[non_exhaustive]
pub struct VoiceReference {
    /// Reference id.
    pub id: i64,
    /// The voice it belongs to.
    pub voice_id: i64,
    /// File name.
    #[serde(default)]
    pub name: Option<String>,
    /// Transcript of the reference audio.
    #[serde(default)]
    pub reference_text: Option<String>,
    /// Storage path.
    #[serde(default)]
    pub s3_path: Option<String>,
    /// Signed URL of the audio, when available.
    #[serde(default)]
    pub audio_url: Option<String>,
    /// Whether the reference was generated rather than uploaded.
    #[serde(default, deserialize_with = "null_as_default")]
    pub is_generated: bool,
}

/// The stored preview sample returned by
/// [`VoicesResource::generate_sample`](crate::VoicesResource::generate_sample).
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[non_exhaustive]
pub struct GeneratedSample {
    /// Storage path of the sample.
    pub sample_s3_path: String,
    /// Signed, time-limited URL; `None` when the server could not sign one.
    #[serde(default)]
    pub sample_url: Option<String>,
}

/// A per-project pronunciation dictionary.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[non_exhaustive]
pub struct Dictionary {
    /// Dictionary id.
    pub id: i64,
    /// Owning project.
    pub project_id: i64,
    /// Name, unique within the project.
    pub name: String,
    /// Description.
    #[serde(default)]
    pub description: Option<String>,
    /// Language the dictionary applies to; `None` for every language.
    #[serde(default)]
    pub language: Option<String>,
    /// Whether the dictionary applies by default.
    #[serde(default = "default_true", deserialize_with = "null_as_true")]
    pub is_active: bool,
    /// Creation time (ISO 8601).
    #[serde(default)]
    pub created_at: Option<String>,
    /// Last update time (ISO 8601).
    #[serde(default)]
    pub updated_at: Option<String>,
}

/// One word to replacement / IPA mapping.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[non_exhaustive]
pub struct DictionaryEntry {
    /// Entry id.
    pub id: i64,
    /// The dictionary it belongs to.
    pub dictionary_id: i64,
    /// The word to replace.
    pub word: String,
    /// What to say instead.
    pub replacement: String,
    /// Optional IPA pronunciation.
    #[serde(default)]
    pub ipa: Option<String>,
    /// Whether matching is case-sensitive.
    #[serde(default, deserialize_with = "null_as_default")]
    pub case_sensitive: bool,
    /// Creation time (ISO 8601).
    #[serde(default)]
    pub created_at: Option<String>,
    /// Last update time (ISO 8601).
    #[serde(default)]
    pub updated_at: Option<String>,
}

/// One page of dictionary entries.
#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
#[non_exhaustive]
pub struct DictionaryEntryList {
    /// The entries on this page.
    #[serde(default)]
    pub entries: Vec<DictionaryEntry>,
    /// Total matching entries.
    pub total: u64,
    /// Page size.
    pub limit: u64,
    /// Page offset.
    pub offset: u64,
}

/// Counts returned by
/// [`DictionaryEntriesResource::replace_all`](crate::DictionaryEntriesResource::replace_all).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[non_exhaustive]
pub struct BulkReplaceResult {
    /// Entries created or updated.
    pub upserted: u64,
    /// Entries removed.
    pub deleted: u64,
    /// Entries in the dictionary afterwards.
    pub total: u64,
}

/// One entry for
/// [`DictionaryEntriesResource::replace_all`](crate::DictionaryEntriesResource::replace_all).
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DictionaryEntryInput {
    /// The word to replace.
    pub word: String,
    /// What to say instead.
    pub replacement: String,
    /// Optional IPA pronunciation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ipa: Option<String>,
    /// Case-sensitive matching; `None` uses the server default (`false`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub case_sensitive: Option<bool>,
}

impl DictionaryEntryInput {
    /// An entry replacing `word` with `replacement`.
    pub fn new(word: impl Into<String>, replacement: impl Into<String>) -> Self {
        Self {
            word: word.into(),
            replacement: replacement.into(),
            ipa: None,
            case_sensitive: None,
        }
    }

    /// Set the IPA pronunciation.
    pub fn ipa(mut self, ipa: impl Into<String>) -> Self {
        self.ipa = Some(ipa.into());
        self
    }

    /// Set case-sensitive matching.
    pub fn case_sensitive(mut self, case_sensitive: bool) -> Self {
        self.case_sensitive = Some(case_sensitive);
        self
    }
}

fn default_true() -> bool {
    true
}

/// `null` and a missing field both mean the type's default.
fn null_as_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Default + Deserialize<'de>,
{
    Ok(Option::<T>::deserialize(deserializer)?.unwrap_or_default())
}

fn null_as_true<'de, D: Deserializer<'de>>(deserializer: D) -> Result<bool, D::Error> {
    Ok(Option::<bool>::deserialize(deserializer)?.unwrap_or(true))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn voice_parses_nulls_and_unknown_enums() {
        let voice: Voice = serde_json::from_value(serde_json::json!({
            "id": 1071, "voice_id": 1071, "handle": "emma", "name": "Emma",
            "description": null, "category": "brand_new", "sex": "female",
            "age": "middle_age", "supported_languages": null, "is_public": null
        }))
        .unwrap();
        assert_eq!(
            voice.category,
            Some(VoiceCategory::Other("brand_new".into()))
        );
        assert_eq!(voice.sex, Some(VoiceSex::Female));
        assert_eq!(voice.age, Some(VoiceAge::MiddleAge));
        assert!(voice.supported_languages.is_empty());
        assert!(!voice.is_public);
        assert_eq!(voice.handle.as_deref(), Some("emma"));
    }

    #[test]
    fn model_tolerates_null_parameters() {
        let model: Model = serde_json::from_value(serde_json::json!({
            "id": "kugel-3.5", "model_id": "kugel-3.5", "name": "Kugel 3.5",
            "description": "Most Natural", "parameters": null,
            "max_input_length": 10000, "sample_rate": 24000
        }))
        .unwrap();
        assert_eq!(model.parameters, None);
        assert_eq!(model.max_input_length, Some(10000));
    }

    #[test]
    fn voice_id_serializes_as_number_or_string() {
        assert_eq!(serde_json::to_string(&VoiceId::from(1071)).unwrap(), "1071");
        assert_eq!(
            serde_json::to_string(&VoiceId::from("emma")).unwrap(),
            "\"emma\""
        );
    }

    #[test]
    fn dictionary_defaults_active() {
        let d: Dictionary = serde_json::from_value(serde_json::json!({
            "id": 1, "project_id": 2, "name": "Brands"
        }))
        .unwrap();
        assert!(d.is_active);
        let entry = serde_json::to_value(DictionaryEntryInput::new("K8s", "kubernetes")).unwrap();
        assert_eq!(
            entry,
            serde_json::json!({"word": "K8s", "replacement": "kubernetes"})
        );
    }
}
