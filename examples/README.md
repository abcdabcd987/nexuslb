# Running NexusLB with the Simple Example

## Build NexusLB

```bash
git clone https://github.com/abcdabcd987/nexuslb.git
cd nexuslb
export NEXUSLB_DIR=$(pwd)
```

See [`BUILDING.md`](../BUILDING.md) for more details.

## Download Model Zoo

```bash
git clone https://gitlab.cs.washington.edu/syslab/nexuslb-model.git
cd nexuslb-model
export MODEL_DIR=$(pwd)
git lfs checkout
```

## Profile ResNet-50 on GPU 0

```bash
python3 $NEXUSLB_DIR/tools/profiler/profiler.py \
    --gpu_list=0 --gpu_uuid --model_root=$MODEL_DIR
    --framework=tensorflow --model=resnet_0 --width=224 --height=224
```

## Run NexusLB Scheduler and Backend, and Application Frontend

```bash
$NEXUSLB_DIR/build/dispatcher \
    -model_root $MODEL_DIR -rdma_dev mlx4_0 -pin_cpus=1-2 -port 7001

$NEXUSLB_DIR/build/backend \
    -model_root $MODEL_DIR -rdma_dev mlx4_0 -sch_addr 127.0.0.1:7001 \
    -gpu 0 -port 8001

$NEXUSLB_DIR/build/simple \
    -rdma_dev mlx4_0 -sch_addr 127.0.0.1:7001 \
    -framework tensorflow -model resnet_0 -latency 100 \
    -height 224 -width 224
```

## Send a Client Request

```bash
wget https://upload.wikimedia.org/wikipedia/commons/4/4c/Chihuahua1_bvdb.jpg

env PYTHONPATH=$NEXUSLB_DIR/python \
    python3 $NEXUSLB_DIR/examples/simple_app/src/client.py \
        --server=localhost:9001 Chihuahua1_bvdb.jpg
```

The [image](https://upload.wikimedia.org/wikipedia/commons/4/4c/Chihuahua1_bvdb.jpg)
should be classified as a *chihuahua*.
