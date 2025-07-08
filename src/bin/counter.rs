use {
    anyhow::Context,
    clap::Parser,
    futures::future::{BoxFuture, FutureExt, pending},
    indicatif::{MultiProgress, ProgressBar, ProgressStyle},
    prost::Message,
    serde::Deserialize,
    solana_sdk::transaction::{TransactionError, VersionedTransaction},
    solana_storage_proto::convert::generated,
    std::collections::VecDeque,
    tokio::{fs::File, io::BufReader, sync::mpsc, task::spawn_blocking},
    yellowstone_faithful_car_parser::node::{
        Node, NodeError, NodeReader, NodeWithCid, Nodes, RawNode,
    },
};

#[derive(Debug, Parser)]
#[clap(author, version, about = "count nodes in CAR files")]
struct Args {
    /// Path to CAR file
    #[clap(long)]
    pub car: String,

    /// Parse Nodes from CAR file
    #[clap(long)]
    pub parse: bool,

    /// Decode Nodes to Solana structs
    #[clap(long)]
    pub decode: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let file = File::open(args.car)
        .await
        .context("failed to open CAR file")?;
    let mut reader = NodeReader::new(BufReader::new(file));

    if !args.parse {
        let bar = ProgressBar::no_length()
            .with_style(ProgressStyle::with_template("{spinner} {pos}").expect("valid template"));
        let mut counter = 0;
        while reader.read_node().await?.is_some() {
            counter += 1;
            if counter >= 131072 {
                bar.inc(counter);
                counter = 0;
            }
        }
        bar.inc(counter);
        bar.finish();
        return Ok(());
    }

    let (read_tx, read_rx) = mpsc::channel(32_768);
    let (node_tx, mut node_rx) = mpsc::channel(32_768);
    tokio::spawn(async move {
        loop {
            let msg = match reader.read_node().await {
                Ok(Some(node)) => Ok(node),
                Ok(None) => break,
                Err(error) => Err(error),
            };
            if read_tx.send(msg).await.is_err() {
                break;
            }
        }
    });
    tokio::spawn(read_and_parse(read_rx, node_tx, 128));

    let mut next_slot = None;
    let mut bar = ReaderProgressBar::new(args.decode);
    loop {
        let mut nodes = Nodes::default();
        let mut finished = false;
        while !finished {
            let node = match node_rx.recv().await {
                Some(Ok(node)) => node,
                Some(Err(error)) => return Err(error.into()),
                None => break,
            };
            finished = matches!(node.node, Node::Block(_));
            nodes.push(node);
        }
        // let nodes = Nodes::read_until_block(&mut reader).await?;
        if nodes.nodes.is_empty() {
            break;
        }

        for node in nodes.nodes.values() {
            match node {
                Node::Transaction(frame) => {
                    bar.transaction += 1;
                    if !args.decode {
                        continue;
                    }

                    let _tx = bincode::deserialize::<VersionedTransaction>(&frame.data.data)
                        .context("failed to parse tx")?;

                    let buffer = nodes
                        .reassemble_dataframes(&frame.metadata)
                        .context("failed to reassemble tx metadata")?;
                    if buffer.is_empty() {
                        bar.transaction_meta_empty += 1;
                    } else {
                        let buffer = zstd::decode_all(buffer.as_slice())
                            .context("failed to decompress tx metadata")?;
                        if decode_protobuf_bincode::<
                            Vec<StoredTransactionStatusMeta>,
                            generated::TransactionStatusMeta,
                        >("tx metadata", &buffer)
                        .is_ok()
                        {
                            bar.transaction_decode_ok += 1;
                        } else {
                            bar.transaction_decode_err += 1;
                        }
                    }
                }
                Node::Entry(_) => bar.entry += 1,
                Node::Block(frame) => {
                    bar.block += 1;

                    let expected_slot = match next_slot {
                        Some(slot) => slot,
                        None => frame.slot - frame.slot % 432_000,
                    };
                    next_slot = Some(frame.slot + 1);
                    bar.block_skippped += frame.slot - expected_slot;
                }
                Node::Subset(_) => bar.subset += 1,
                Node::Epoch(_) => bar.epoch += 1,
                Node::Rewards(frame) => {
                    bar.rewards += 1;
                    if !args.decode {
                        continue;
                    }

                    let buffer = nodes
                        .reassemble_dataframes(&frame.data)
                        .context("failed to reassemble rewards")?;
                    let buffer = zstd::decode_all(buffer.as_slice())
                        .context("failed to decompress rewards")?;
                    if decode_protobuf_bincode::<Vec<StoredBlockReward>, generated::Rewards>(
                        "rewards", &buffer,
                    )
                    .is_ok()
                    {
                        bar.rewards_decode_ok += 1;
                    } else {
                        bar.rewards_decode_err += 1;
                    }
                }
                Node::DataFrame(_) => bar.dataframe += 1,
            }
        }

        bar.report();
    }
    bar.finish();

    Ok(())
}

struct ReaderProgressBar {
    transaction: u64,
    pb_transaction: ProgressBar,
    entry: u64,
    pb_entry: ProgressBar,
    block: u64,
    pb_block: ProgressBar,
    subset: u64,
    pb_subset: ProgressBar,
    epoch: u64,
    pb_epoch: ProgressBar,
    rewards: u64,
    pb_rewards: ProgressBar,
    dataframe: u64,
    pb_dataframe: ProgressBar,
    //
    block_skippped: u64,
    pb_block_skipped: ProgressBar,
    //
    transaction_meta_empty: u64,
    pb_transaction_meta_empty: Option<ProgressBar>,
    transaction_decode_ok: u64,
    pb_transaction_decode_ok: Option<ProgressBar>,
    transaction_decode_err: u64,
    pb_transaction_decode_err: Option<ProgressBar>,
    rewards_decode_ok: u64,
    pb_rewards_decode_ok: Option<ProgressBar>,
    rewards_decode_err: u64,
    pb_rewards_decode_err: Option<ProgressBar>,
}

impl ReaderProgressBar {
    fn new(decode: bool) -> Self {
        let multi = MultiProgress::new();
        Self {
            transaction: 0,
            pb_transaction: Self::create_pbbar(&multi, "parsed", "transaction"),
            entry: 0,
            pb_entry: Self::create_pbbar(&multi, "parsed", "entry"),
            block: 0,
            pb_block: Self::create_pbbar(&multi, "parsed", "block"),
            subset: 0,
            pb_subset: Self::create_pbbar(&multi, "parsed", "subset"),
            epoch: 0,
            pb_epoch: Self::create_pbbar(&multi, "parsed", "epoch"),
            rewards: 0,
            pb_rewards: Self::create_pbbar(&multi, "parsed", "rewards"),
            dataframe: 0,
            pb_dataframe: Self::create_pbbar(&multi, "parsed", "dataframe"),
            //
            block_skippped: 0,
            pb_block_skipped: Self::create_pbbar(&multi, "skipped", "block"),
            //
            transaction_meta_empty: 0,
            pb_transaction_meta_empty: decode
                .then(|| Self::create_pbbar(&multi, "meta_empty", "transaction")),
            transaction_decode_ok: 0,
            pb_transaction_decode_ok: decode
                .then(|| Self::create_pbbar(&multi, "decoded/ok", "transaction")),
            transaction_decode_err: 0,
            pb_transaction_decode_err: decode
                .then(|| Self::create_pbbar(&multi, "decoded/err", "transaction")),
            rewards_decode_ok: 0,
            pb_rewards_decode_ok: decode
                .then(|| Self::create_pbbar(&multi, "decoded/ok", "rewards")),
            rewards_decode_err: 0,
            pb_rewards_decode_err: decode
                .then(|| Self::create_pbbar(&multi, "decoded/err", "rewards")),
        }
    }

    fn create_pbbar(pb: &MultiProgress, kind1: &str, kind2: &str) -> ProgressBar {
        let pb = pb.add(ProgressBar::no_length());
        pb.set_style(
            ProgressStyle::with_template(&format!("{{spinner}} {kind1}:{kind2} {{pos}}"))
                .expect("valid template"),
        );
        pb
    }

    fn report(&self) {
        for (pb, pos) in [
            (Some(&self.pb_transaction), self.transaction),
            (Some(&self.pb_entry), self.entry),
            (Some(&self.pb_block), self.block),
            (Some(&self.pb_subset), self.subset),
            (Some(&self.pb_epoch), self.epoch),
            (Some(&self.pb_rewards), self.rewards),
            (Some(&self.pb_dataframe), self.dataframe),
            //
            (Some(&self.pb_block_skipped), self.block_skippped),
            //
            (
                self.pb_transaction_meta_empty.as_ref(),
                self.transaction_meta_empty,
            ),
            (
                self.pb_transaction_decode_ok.as_ref(),
                self.transaction_decode_ok,
            ),
            (
                self.pb_transaction_decode_err.as_ref(),
                self.transaction_decode_err,
            ),
            (self.pb_rewards_decode_ok.as_ref(), self.rewards_decode_ok),
            (self.pb_rewards_decode_err.as_ref(), self.rewards_decode_err),
        ] {
            if let Some(pb) = pb {
                pb.set_position(pos);
            }
        }
    }

    fn finish(&self) {
        for pb in [
            Some(&self.pb_transaction),
            Some(&self.pb_entry),
            Some(&self.pb_block),
            Some(&self.pb_subset),
            Some(&self.pb_epoch),
            Some(&self.pb_rewards),
            Some(&self.pb_dataframe),
            //
            Some(&self.pb_block_skipped),
            //
            self.pb_transaction_meta_empty.as_ref(),
            self.pb_transaction_decode_ok.as_ref(),
            self.pb_transaction_decode_err.as_ref(),
            self.pb_rewards_decode_ok.as_ref(),
            self.pb_rewards_decode_err.as_ref(),
        ]
        .into_iter()
        .flatten()
        {
            pb.finish();
        }
    }
}

enum DecodedData<B, P> {
    Bincode(B),
    Protobuf(P),
}

fn decode_protobuf_bincode<B, P>(kind: &str, bytes: &[u8]) -> anyhow::Result<DecodedData<B, P>>
where
    B: serde::de::DeserializeOwned,
    P: Message + Default,
{
    match P::decode(bytes) {
        Ok(value) => Ok(DecodedData::Protobuf(value)),
        Err(_) => bincode::deserialize::<B>(bytes)
            .map(DecodedData::Bincode)
            .with_context(|| format!("failed to decode {kind} with protobuf/bincode")),
    }
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct StoredTransactionStatusMeta {
    err: Option<TransactionError>,
    fee: u64,
    pre_balances: Vec<u64>,
    post_balances: Vec<u64>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct StoredBlockReward {
    pubkey: String,
    lamports: i64,
}

async fn read_and_parse(
    rx: mpsc::Receiver<Result<RawNode, NodeError>>,
    tx: mpsc::Sender<Result<NodeWithCid, NodeError>>,
    buffer_size: usize,
) {
    let mut queue: VecDeque<
        BoxFuture<'static, Result<Result<NodeWithCid, NodeError>, tokio::task::JoinError>>,
    > = VecDeque::with_capacity(buffer_size);

    let node_fut = pending().boxed();
    tokio::pin!(node_fut);
    let mut node_fut_assigned = false;

    let mut rx = Some(rx);
    while rx.is_some() {
        let rx_fut = match &mut rx {
            Some(rx) if queue.len() < buffer_size => rx.recv().boxed(),
            _ => pending().boxed(),
        };

        if !node_fut_assigned {
            if let Some(fut) = queue.pop_front() {
                node_fut.set(fut.boxed());
                node_fut_assigned = true;
            }
        }

        tokio::select! {
            recv_msg = rx_fut => match recv_msg {
                Some(msg) => {
                    queue.push_back(spawn_blocking(move || NodeWithCid::try_from(&(msg?))).boxed());
                },
                None => {
                    rx = None;
                }
            },
            msg = &mut node_fut => {
                tx.send(msg.expect("failed to join spawned task")).await.expect("failed to send a msg");
                node_fut.set(pending().boxed());
                node_fut_assigned = false;
            }
        };
    }

    if node_fut_assigned {
        tx.send(node_fut.await.expect("failed to join spawned task"))
            .await
            .expect("failed to send a msg");
    }

    while let Some(node_fut) = queue.pop_front() {
        tx.send(node_fut.await.expect("failed to join spawned task"))
            .await
            .expect("failed to send a msg");
    }
}
