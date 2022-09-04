use crate::consumer::Consumer;
use crate::context::Context;
use crate::kafka::config::StreamConsumerConfig;
use crate::kafka::consumer::IStreamConsumer;
use crate::options::AnyOptions;
use crate::processor::{DefaultProcessor, IProcessor};
use log::{error, info};
use std::future::Future;
use std::sync::Arc;
use tokio::sync::{broadcast, mpsc, Mutex};

/// [`MsgProc`](crate::msgproc::MsgProc)の設定
pub struct MsgProcConfig {
    consumer_config: Option<StreamConsumerConfig>,
    topics: Vec<String>,
    processor: Arc<Mutex<Box<dyn IProcessor>>>,
    context: Context,
    shutdown_complete_rx: Option<mpsc::Receiver<()>>,
    shutdown_complete_tx: Option<mpsc::Sender<()>>,
    options: AnyOptions,
}

impl MsgProcConfig {
    const PROCESSOR_BUFFER_SIZE: &'static str = "processor_buffer_size";

    /// 空の設定を新規作成する
    pub fn new() -> Self {
        Self::default()
    }

    /// [`IStreamConsumer`](crate::kafka::consumer::IStreamConsumer)の設定値を設定する
    ///
    /// すでにkeyが設定されている場合、上書きが行われる。
    pub fn set_stream_consumer_param<K, V>(&mut self, key: K, value: V) -> &mut Self
    where
        K: Into<String>,
        V: Into<String>,
    {
        self.consumer_config
            .as_mut()
            .unwrap()
            .set(key.into(), value.into());
        self
    }

    /// [`IStreamConsumer`](crate::kafka::consumer::IStreamConsumer)で購読するトピックを設定する
    pub fn set_topics(&mut self, topics: &[&str]) -> &mut Self {
        self.topics = topics.iter().map(|x| x.to_string()).collect::<Vec<_>>();
        self
    }

    /// Kafkaから取得したメッセージを[`MessageStream`](crate::kafka::consumer::MessageStream)を通じて[`Consumer`](crate::consumer::Consumer)で取得する際に使用するチャネルの上限を設定する
    pub fn set_consumer_buffer_size(&mut self, consumer_buffer_size: usize) -> &mut Self {
        self.consumer_config
            .as_mut()
            .unwrap()
            .set_buffer_size(consumer_buffer_size);
        self
    }

    /// [`MessageStream`](crate::kafka::consumer::MessageStream)から取得したメッセージを[`IProcessor`](crate::processor::IProcessor)に送信する際に使用するチャネルの上限を設定する
    pub fn set_processor_buffer_size(&mut self, processor_buffer_size: usize) -> &mut Self {
        self.options
            .set(Self::PROCESSOR_BUFFER_SIZE, processor_buffer_size);
        self
    }

    /// [`IProcessor`](crate::processor::IProcessor)を設定する
    pub fn set_processor(&mut self, processor: impl IProcessor) -> &mut Self {
        self.processor = Arc::new(Mutex::new(Box::new(processor)));
        self
    }

    /// 現在の設定値を使用して[`MsgProc`](crate::msgproc::MsgProc)を作成する
    pub fn create(&mut self) -> MsgProc {
        let buffer_size = self.options.get(Self::PROCESSOR_BUFFER_SIZE);
        MsgProc {
            consumer: Arc::new(Box::new(
                self.consumer_config.clone().unwrap().create().unwrap(),
            )),
            context: self.context.clone(),
            shutdown_complete_rx: self.shutdown_complete_rx.take(),
            shutdown_complete_tx: self.shutdown_complete_tx.take(),
            processor: self.processor.clone(),
            buffer_size,
        }
    }
}

impl Default for MsgProcConfig {
    fn default() -> Self {
        let mut options = AnyOptions::new();
        options.set::<usize>(Self::PROCESSOR_BUFFER_SIZE, 64);
        let (notify_shutdown, _) = broadcast::channel(1);
        let context = Context::new(notify_shutdown.clone());
        let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel(1);
        let processor: Arc<Mutex<Box<dyn IProcessor>>> =
            Arc::new(Mutex::new(Box::new(DefaultProcessor)));
        Self {
            consumer_config: Some(StreamConsumerConfig::new()),
            topics: vec![],
            processor,
            context,
            shutdown_complete_rx: Some(shutdown_complete_rx),
            shutdown_complete_tx: Some(shutdown_complete_tx),
            options,
        }
    }
}

/// Kafkaから消費したメッセージを処理する
///
/// メッセージを消費し、あらかじめ登録したprocessorにメッセージを配送する。
pub struct MsgProc {
    consumer: Arc<Box<dyn IStreamConsumer>>,
    context: Context,
    shutdown_complete_rx: Option<mpsc::Receiver<()>>,
    shutdown_complete_tx: Option<mpsc::Sender<()>>,
    processor: Arc<Mutex<Box<dyn IProcessor>>>,
    buffer_size: usize,
}

impl MsgProc {
    /// [`MsgProc`](MsgProc)を実行する
    ///
    /// * `shutdown` - 実行の中断を行うためのFuture
    pub async fn run(self, shutdown: impl Future + Send) {
        let mut consumer = Consumer::new(
            self.consumer,
            self.context,
            self.shutdown_complete_rx.unwrap(),
            self.shutdown_complete_tx.unwrap(),
            self.processor,
            self.buffer_size,
        );
        info!("Start processing.");
        tokio::select! {
            _ = consumer.run_main_consume() => {
                error!("Error occurred.");
            }
            _ = shutdown => {
                info!("Catch shutdown event. Shutdowning...");
            }
        }
        consumer.wait().await;
    }
}
