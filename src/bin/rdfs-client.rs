use rdfs::client::{Client, WriteOptions};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() < 3 {
        eprintln!("usage: rdfs-client <meta1,meta2,...> <command> [args]");
        eprintln!(
            "commands: demo | mkdir <path> | ls <path> | put <path> <text> | cat <path> | rm <path> | stat <path> | meta-membership | meta-add <id> <addr> [--voter] | meta-rm <id> [--retain] | meta-replace <old_id> <new_id> <new_addr>"
        );
        std::process::exit(1);
    }

    let client = Client::new(
        args[1]
            .split(',')
            .filter(|entry| !entry.trim().is_empty())
            .map(|entry| entry.to_string()),
    )?;

    match args[2].as_str() {
        "demo" => {
            let _ = client.mkdir("/demo").await;
            let mut writer = client
                .create_writer("/demo/demo.txt", WriteOptions::default())
                .await?;
            writer.write(b"rdfs v3 demo").await?;
            writer.commit().await?;
            let reader = client.open_reader("/demo/demo.txt").await?;
            println!("{}", String::from_utf8_lossy(&reader.read_all().await?));
        }
        "mkdir" => {
            client.mkdir(args.get(3).expect("missing path")).await?;
        }
        "ls" => {
            for entry in client.list(args.get(3).expect("missing path")).await? {
                println!(
                    "{} {}",
                    if entry.is_dir { "dir " } else { "file" },
                    entry.path
                );
            }
        }
        "put" => {
            let path = args.get(3).expect("missing path");
            let body = args.get(4).expect("missing text");
            let mut writer = if client.stat(path).await.is_ok() {
                client
                    .overwrite_writer(path, WriteOptions::default())
                    .await?
            } else {
                client.create_writer(path, WriteOptions::default()).await?
            };
            writer.write(body.as_bytes()).await?;
            writer.commit().await?;
        }
        "cat" => {
            let reader = client
                .open_reader(args.get(3).expect("missing path"))
                .await?;
            println!("{}", String::from_utf8_lossy(&reader.read_all().await?));
        }
        "rm" => {
            client.delete(args.get(3).expect("missing path")).await?;
        }
        "stat" => {
            let info = client.stat(args.get(3).expect("missing path")).await?;
            println!("{info:?}");
        }
        "meta-membership" => {
            print_membership(&client.cluster_membership().await?);
        }
        "meta-add" => {
            let node_id = args.get(3).expect("missing node id").parse::<u64>()?;
            let addr = args.get(4).expect("missing addr").clone();
            let promote = args.iter().any(|arg| arg == "--voter");
            let membership = client.add_metadata_node(node_id, addr, promote).await?;
            print_membership(&membership);
        }
        "meta-rm" => {
            let node_id = args.get(3).expect("missing node id").parse::<u64>()?;
            let retain = args.iter().any(|arg| arg == "--retain");
            let membership = client.remove_metadata_node(node_id, retain).await?;
            print_membership(&membership);
        }
        "meta-replace" => {
            let old_node_id = args.get(3).expect("missing old node id").parse::<u64>()?;
            let new_node_id = args.get(4).expect("missing new node id").parse::<u64>()?;
            let new_addr = args.get(5).expect("missing new addr").clone();
            let membership = client
                .replace_metadata_node(old_node_id, new_node_id, new_addr)
                .await?;
            print_membership(&membership);
        }
        other => anyhow::bail!("unknown command: {other}"),
    }

    Ok(())
}

fn print_membership(membership: &rdfs::pb::ClusterMembership) {
    let mut nodes = membership.nodes.clone();
    nodes.sort_by_key(|node| node.id);
    for node in nodes {
        println!(
            "{} {} {}",
            if node.voter { "voter" } else { "learner" },
            node.id,
            node.addr
        );
    }
    if membership.joint {
        println!("joint true");
    }
}
