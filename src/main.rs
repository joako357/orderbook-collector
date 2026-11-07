//! Collects every live order book of one exchange and writes every price level change, with the
//! level's new quantity, plus a full snapshot every minute, to stdout in the template's SBE
//! format. SIGINT or SIGTERM closes the websockets and the output.
//!
//! Usage: `engineering-challenge polymarket|kalshi|kalshi-demo > books.sbe`. Kalshi reads its API
//! key from `KALSHI_KEY_ID` and `KALSHI_KEY_FILE` (the Ed25519 or RSA private key, in PEM).

use anyhow::Context;
use tokio::runtime::Runtime;
use tokio::signal::unix::{SignalKind, signal};

use engineering_challenge::exchange::kalshi::Kalshi;
use engineering_challenge::exchange::polymarket::Polymarket;
use engineering_challenge::service::serve;

const STDOUT: &str = "/dev/stdout";

fn main() -> anyhow::Result<()> {
    let argument = std::env::args().nth(1);
    let exchange = chosen(argument.as_deref(), |name| std::env::var(name).ok())?;
    let runtime = Runtime::new()?;
    let (interrupt, interrupted) = crossbeam::channel::bounded(1);
    let mut terminate = {
        let _context = runtime.enter();
        signal(SignalKind::terminate())?
    };
    runtime.spawn(async move {
        tokio::select! {
            Ok(()) = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
        let _ = interrupt.send(());
    });
    match exchange {
        Chosen::Polymarket(polymarket) => serve(polymarket, &runtime, STDOUT, interrupted),
        Chosen::Kalshi(kalshi) => serve(*kalshi, &runtime, STDOUT, interrupted),
    }
}

/// The exchange the service collects.
enum Chosen {
    Polymarket(Polymarket),
    Kalshi(Box<Kalshi>),
}

/// The exchange `argument` names. Kalshi's API key comes from the environment, which `var`
/// reads: the key's id from `KALSHI_KEY_ID`, and its private key from the file `KALSHI_KEY_FILE`
/// names.
fn chosen(argument: Option<&str>, var: impl Fn(&str) -> Option<String>) -> anyhow::Result<Chosen> {
    let kalshi_key = || -> anyhow::Result<(String, String)> {
        let key_id = var("KALSHI_KEY_ID").context("KALSHI_KEY_ID is not set")?;
        let file = var("KALSHI_KEY_FILE").context("KALSHI_KEY_FILE is not set")?;
        let key = std::fs::read_to_string(&file).with_context(|| format!("reading {file}"))?;
        Ok((key_id, key))
    };
    Ok(match argument {
        Some("polymarket") => Chosen::Polymarket(Polymarket::new()?),
        Some("kalshi") => {
            let (key_id, key) = kalshi_key()?;
            Chosen::Kalshi(Box::new(Kalshi::production(key_id, &key)?))
        }
        Some("kalshi-demo") => {
            let (key_id, key) = kalshi_key()?;
            Chosen::Kalshi(Box::new(Kalshi::demo(key_id, &key)?))
        }
        _ => {
            anyhow::bail!("usage: engineering-challenge polymarket|kalshi|kalshi-demo > books.sbe")
        }
    })
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use ed25519_dalek::pkcs8::EncodePrivateKey as _;
    use engineering_challenge::exchange::Exchange;

    use super::*;

    /// A file holding a throwaway Ed25519 key, in the PKCS#8 PEM Kalshi hands out.
    fn key_file() -> tempfile::NamedTempFile {
        let key = ed25519_dalek::SigningKey::from_bytes(&[7; 32]);
        let pem = key.to_pkcs8_pem(Default::default()).unwrap();
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(pem.as_bytes()).unwrap();
        file
    }

    /// An environment that holds `vars` and nothing else.
    fn env<'a>(vars: &'a [(&str, &str)]) -> impl Fn(&str) -> Option<String> + 'a {
        |name| {
            let value = vars.iter().find(|(var, _)| *var == name);
            value.map(|(_, value)| value.to_string())
        }
    }

    /// The websocket the chosen exchange connects to, and the key id it sends.
    fn websocket(chosen: anyhow::Result<Chosen>) -> (String, Option<String>) {
        let request = match chosen.unwrap() {
            Chosen::Polymarket(polymarket) => polymarket.request(),
            Chosen::Kalshi(kalshi) => kalshi.request(),
        };
        let request = request.unwrap();
        let key_id = request.headers().get("KALSHI-ACCESS-KEY");
        let key_id = key_id.map(|id| id.to_str().unwrap().to_owned());
        (request.uri().to_string(), key_id)
    }

    fn error(chosen: anyhow::Result<Chosen>) -> String {
        let Err(error) = chosen else {
            panic!("the choice should fail");
        };
        format!("{error:#}")
    }

    #[test]
    fn polymarket_needs_no_key() {
        assert_eq!(
            websocket(chosen(Some("polymarket"), env(&[]))),
            (
                "wss://ws-subscriptions-clob.polymarket.com/ws/market".into(),
                None
            )
        );
    }

    #[test]
    fn kalshi_and_its_demo_read_the_key_from_the_environment() {
        let file = key_file();
        let path = file.path().to_str().unwrap();
        let vars = [("KALSHI_KEY_ID", "key-id"), ("KALSHI_KEY_FILE", path)];
        let production = "wss://external-api-ws.kalshi.com/trade-api/ws/v2";
        let demo = "wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2";
        for (argument, host) in [("kalshi", production), ("kalshi-demo", demo)] {
            assert_eq!(
                websocket(chosen(Some(argument), env(&vars))),
                (host.into(), Some("key-id".into()))
            );
        }
    }

    #[test]
    fn kalshi_without_its_key_says_what_is_missing() {
        let file = key_file();
        let path = file.path().to_str().unwrap();
        let missing = [
            (vec![("KALSHI_KEY_FILE", path)], "KALSHI_KEY_ID is not set"),
            (
                vec![("KALSHI_KEY_ID", "key-id")],
                "KALSHI_KEY_FILE is not set",
            ),
            (
                vec![
                    ("KALSHI_KEY_ID", "key-id"),
                    ("KALSHI_KEY_FILE", "no/such.pem"),
                ],
                "reading no/such.pem",
            ),
        ];
        for (vars, expected) in missing {
            let error = error(chosen(Some("kalshi"), env(&vars)));
            assert!(error.contains(expected), "{error}");
        }
    }

    #[test]
    fn a_missing_or_unknown_exchange_gets_the_usage() {
        for argument in [None, Some("binance")] {
            assert!(error(chosen(argument, env(&[]))).starts_with("usage: "));
        }
    }
}
