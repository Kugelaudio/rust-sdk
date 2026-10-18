//! Resource accessors returned by [`KugelAudio`](crate::KugelAudio).

mod dictionaries;
mod enhance;
mod tts;
mod voices;

pub use dictionaries::{
    DictionariesResource, DictionaryCreate, DictionaryEntriesResource, DictionaryEntryUpdate,
    DictionaryUpdate, EntryListOptions, NewDictionaryEntry,
};
pub use enhance::{EnhanceOptions, EnhanceResource, EnhanceSession, EnhancedAudio, PcmInput};
pub use tts::{StreamEvent, TtsResource, TtsStream};
pub use voices::{ReferenceFile, VoiceCreate, VoiceListOptions, VoiceUpdate, VoicesResource};

use reqwest::Method;

use crate::client::KugelAudio;
use crate::error::Result;
use crate::models::Model;

/// The model catalog, from [`KugelAudio::models`].
#[derive(Debug, Clone, Copy)]
pub struct ModelsResource<'a> {
    client: &'a KugelAudio,
}

#[derive(serde::Deserialize)]
struct ModelList {
    #[serde(default)]
    models: Vec<Model>,
}

impl<'a> ModelsResource<'a> {
    pub(crate) fn new(client: &'a KugelAudio) -> Self {
        Self { client }
    }

    /// List the available TTS models (`GET /v1/models`).
    pub async fn list(&self) -> Result<Vec<Model>> {
        let list: ModelList = self
            .client
            .inner
            .json(Method::GET, "/v1/models", &[], None)
            .await?;
        Ok(list.models)
    }
}
