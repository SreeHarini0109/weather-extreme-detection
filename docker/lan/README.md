# Running the cluster on lab PCs (LAN)

Same Docker image as `docker/docker-compose.yml`, but one container per PC with **host networking**, so
every PC keeps its own LAN IP. MPI ranks are launched over SSH on port **2222** (the PC's own sshd keeps
port 22), and Spark runs in standalone mode with the master bound to the master PC's IP.

Tested here: the scripts' syntax, the Compose config, and the LAN code paths (SSH on 2222, `mpirun -p 2222`,
configurable Spark master host) on the 4-container cluster, where MPI reached all nodes, Spark registered
3 workers and results matched the sequential engine. Not tested: real separate PCs (needs your lab).

## Requirements on every PC
* Docker (Linux PCs: Docker Engine; Docker Desktop's host networking is limited on macOS/Windows)
* The PCs can reach each other on TCP 2222 (SSH), 7077 (Spark master), 8080/8081 (Spark UIs) and the
  ephemeral ports MPI and Spark use: same subnet, firewall open between the lab PCs
* The project at the **same path** on every PC (step 2 copies it)

## Steps (example: master 192.168.1.10, workers .11 .12 .13)

1. **Build the image once per PC** (or `docker save` / `docker load` it from the master to save downloads):
   `docker compose -f docker/lan/docker-compose.lan.yml build`
2. **Copy code + data to the workers** (from the master PC):
   `bash docker/lan/sync_project.sh user@192.168.1.11 user@192.168.1.12 user@192.168.1.13`
3. **Start the nodes** (master first):
   * master PC: `ROLE=master bash docker/lan/start_node.sh 192.168.1.10`
   * each worker PC: `ROLE=worker bash docker/lan/start_node.sh 192.168.1.10`
   (`NODE_IP` and `LAN_SUBNET` are auto-detected; set them if a PC has several network cards.)
4. **Write the MPI hostfile** (master first; slots = cores you want to use per PC):
   `bash docker/lan/make_hostfile.sh --slots 4 192.168.1.10 192.168.1.11 192.168.1.12 192.168.1.13`
5. **Check everything end to end:** `bash docker/lan/check_cluster.sh 192.168.1.10`
   It prints one host name per PC from `mpirun`, the number of live Spark workers, then runs the
   sequential, MPI and Spark engines and checks the results are identical.
6. **Benchmarks on the LAN**, for example:
   `docker exec pds-node mpirun --hostfile /app/docker/lan/hostfile -np 16 python3 -m engines.mpi_engine --mode station --tag lan`
   `docker exec pds-node python3 -m engines.spark_engine --master spark://192.168.1.10:7077 --tag lan`
   Rows go to `results/benchmark.csv` with the number of distinct nodes in the `nodes` column.
   Streaming extension: Spark's file source and checkpoint must be on storage every Spark node shares.
   `sync_project.sh` copies files but does not share them, so either mount one network share at the same
   path on every PC, or run the stream on the master PC only:
   `docker exec -w /app pds-node python3 -m streaming.demo --scenario heatwave_2024 --master "local[4]"`
7. **Fault-tolerance demo:** during a Spark run, `docker stop pds-node` on one worker PC. The job still
   completes (lost partitions are recomputed from lineage). Restart it with step 3.
8. **Stop:** `docker compose -f docker/lan/docker-compose.lan.yml down` on every PC.

## Troubleshooting
* `mpirun` hangs: check `ssh -p 2222 root@<worker-ip> hostname` works from inside the master container
  (`docker exec -it pds-node bash`); check `LAN_SUBNET` matches the PCs' network.
* Spark workers missing from `http://<master-ip>:8080`: the worker log (`docker logs pds-node`) shows
  "waiting for Spark master" until 7077 is reachable; open the firewall between the PCs.
* File-not-found on a worker: the project path differs between PCs; re-run step 2.
