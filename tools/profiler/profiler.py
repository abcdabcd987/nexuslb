import math
import multiprocessing
import os
import io
import sys
import argparse
import subprocess
import shutil
import uuid
import numpy as np


_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
_profiler = os.path.join(_root, 'build/profiler')


def parse_int_list(s):
    res = []
    for split in s.split(','):
        if '-' in split:
            st, ed = map(int, split.split('-'))
            res.extend(range(st, ed+1))
        else:
            res.append(int(split))
    return res


def run_profiler(gpu, prof_id, input_queue, output_queue):
    while True:
        cmd = input_queue.get()
        if cmd is None:
            break
        cmd.append(f'-gpu={gpu}')
        print(' '.join(cmd))
        proc = subprocess.Popen(
            cmd, bufsize=0, encoding='utf-8', stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)

        sio = io.StringIO(newline='')
        try:
            while True:
                if proc.poll() is not None:
                    break
                ch = proc.stdout.read(1)
                sio.write(ch)
                sys.stdout.write(ch)
                sys.stdout.flush()
        except KeyboardInterrupt:
            proc.terminate()
            proc.wait()
            raise
        if proc.returncode != 0:
            sys.stderr.write('Profiler exited with ' + str(proc.returncode))
            sys.stderr.flush()
            return

        lines = iter(sio.getvalue().splitlines())
        sio.close()
        for line in lines:
            if line.startswith(prof_id):
                break
        gpu_name = next(lines).strip()
        gpu_uuid = next(lines).strip()
        next(lines)  # Forward latency
        next(lines)  # batch,latency(us),std(us),memory(B),repeat
        forward_stats = []
        for line in lines:
            if line.startswith('Preprocess latency (mean,std,repeat)'):
                break
            batch_size, lat, std, static_mem, peak_mem, repeat = line.split(',')
            forward_stats.append((int(batch_size), float(
                lat), float(std), int(static_mem), int(peak_mem), int(repeat)))

        mean, std, repeat = next(lines).split(',')
        preprocess_lats = (float(mean), float(std), int(repeat))

        next(lines)  # Postprocess latency (mean,std,repeat)
        mean, std, repeat = next(lines).split(',')
        postprocess_lats = (float(mean), float(std), int(repeat))

        output_queue.put((gpu_name, gpu_uuid, forward_stats,
                          preprocess_lats, postprocess_lats))


def print_profile(output, prof_id, gpu_name, gpu_uuid, forward_stats, preprocess_lats, postprocess_lats):
    forward_stats.sort()
    with open(output, 'w') as f:
        f.write(f'{prof_id}\n')
        f.write(f'{gpu_name}\n')
        f.write(f'{gpu_uuid}\n')
        f.write('Forward latency\n')
        f.write('batch,latency(us),std(us),static memory(B),peak memory(B),repeat\n')
        for batch_size, lat, std, static_mem, peak_mem, repeat in forward_stats:
            f.write(f'{batch_size},{lat},{std},{static_mem},{peak_mem},{repeat}\n')
        f.write('Preprocess latency (mean,std,repeat)\n')
        mean, std, repeat = preprocess_lats
        f.write(f'{mean},{std},{repeat}\n')
        f.write('Postprocess latency (mean,std,repeat)\n')
        mean, std, repeat = postprocess_lats
        f.write(f'{mean},{std},{repeat}\n')


def merge_mean_std(tuple1, tuple2):
    mean1, std1, n1 = tuple1
    mean2, std2, n2 = tuple2
    mean = ((n1 - 1) * mean1 + (n2 - 1) * mean2) / (n1 + n2 - 1)
    var = (n1 - 1) * std1 ** 2 + (n2 - 1) * std2 ** 2
    var += n1 * (mean1 - mean) ** 2
    var += n2 * (mean2 - mean) ** 2
    var /= (n1 + n2 - 1)
    return mean, math.sqrt(var), n1 + n2


def get_profiler_cmd(args):
    cmd = [_profiler,
           f'-model_root={args.model_root}',
           f'-framework={args.framework}', f'-model={args.model}', f'-model_version={args.version}']
    if args.height > 0 and args.width > 0:
        cmd.append(f'-height={args.height}')
        cmd.append(f'-width={args.width}')
    return cmd


def profile_model_concurrent(gpus, min_batch, max_batch, prof_id, output):
    input_queue = multiprocessing.Queue(max_batch - min_batch + 1)
    output_queue = multiprocessing.Queue(max_batch - min_batch + 1)
    workers = []
    for gpu in gpus:
        worker = multiprocessing.Process(target=run_profiler, args=(
            gpu, prof_id, input_queue, output_queue))
        worker.start()
        workers.append(worker)
    for batch in range(min_batch, max_batch + 1):
        cmd = get_profiler_cmd(args)
        cmd += [f'-min_batch={batch}', f'-max_batch={batch}']
        input_queue.put(cmd)
    for _ in gpus:
        input_queue.put(None)
    input_queue.close()

    forward_stats = []
    preprocess_lats = None
    postprocess_lats = None
    for _ in range(min_batch, max_batch + 1):
        try:
            gpu_name, gpu_uuid, forward_stat, pre, post = output_queue.get()
        except KeyboardInterrupt:
            print('exiting...')
            for worker in workers:
                worker.terminate()
            output_queue.close()
            for worker in workers:
                worker.join()
            print('worker joined')
            output_queue.join_thread()
            raise
        forward_stats.extend(forward_stat)
        if preprocess_lats is None:
            preprocess_lats = pre
            postprocess_lats = post
        else:
            preprocess_lats = merge_mean_std(preprocess_lats, pre)
            postprocess_lats = merge_mean_std(postprocess_lats, post)
        if len(gpus) > 1:
            gpu_uuid = 'generic'
        print_profile(output, prof_id, gpu_name, gpu_uuid,
                      forward_stats, preprocess_lats, postprocess_lats)

    print('joining worker...')
    for worker in workers:
        worker.join()
    print('worker joined')


def profile_model_single_gpu(gpu, min_batch, max_batch, prof_id, output):
    input_queue = multiprocessing.Queue()
    output_queue = multiprocessing.Queue()
    worker = multiprocessing.Process(target=run_profiler, args=(
        gpu, prof_id, input_queue, output_queue))
    worker.start()
    cmd = get_profiler_cmd(args)
    cmd += [f'-min_batch={min_batch}', f'-max_batch={max_batch}']
    input_queue.put(cmd)
    input_queue.put(None)
    input_queue.close()

    try:
        gpu_name, gpu_uuid, forward_stats, preprocess_lats, postprocess_lats = output_queue.get()
    except KeyboardInterrupt:
        print('exiting...')
        worker.terminate()
        output_queue.close()
        worker.join()
        print('worker joined')
        output_queue.join_thread()
        raise
    print_profile(output, prof_id, gpu_name, gpu_uuid,
                  forward_stats, preprocess_lats, postprocess_lats)

    print('joining worker...')
    worker.join()
    print('worker joined')


def profile_model(args):
    if args.gpu_list == "-1":
        gpus = [-1]
    else:
        gpus = parse_int_list(args.gpu_list)
    if len(gpus) != 1 and args.gpu_uuid:
        raise ValueError('--gpu_uuid cannot be set with more than one --gpus')

    prof_id = '%s:%s:%s' % (args.framework, args.model, args.version)
    if args.height > 0 and args.width > 0:
        prof_id += ':%sx%s' % (args.height, args.width)
    print('Start profiling %s' % prof_id)

    max_batch = args.max_batch
    min_batch = max(1, args.min_batch)
    print('Min batch: %s' % min_batch)
    print('Max batch: %s' % max_batch)
    print('Start profiling. This could take a long time.')

    output = prof_id.replace(':', '-') + '-' + str(uuid.uuid4()) + '.txt'
    if len(gpus) > 1:
        profile_model_concurrent(gpus, min_batch, max_batch, prof_id, output)
    else:
        profile_model_single_gpu(
            gpus[0], min_batch, max_batch, prof_id, output)
    with open(output) as f:
        next(f)
        gpu_name = next(f).strip()
        gpu_uuid = next(f).strip()

    prof_dir = os.path.join(args.model_root, 'profiles')
    assert gpu_name is not None
    gpu_dir = os.path.join(prof_dir, gpu_name)
    if args.gpu_uuid:
        gpu_dir = os.path.join(gpu_dir, gpu_uuid)
    if not os.path.exists(gpu_dir):
        os.makedirs(gpu_dir, exist_ok=True)
    prof_file = os.path.join(gpu_dir, '%s.txt' % prof_id.replace(':', '-'))
    if os.path.exists(prof_file) and not args.force:
        print('%s already exists' % prof_file)
        print('Save profile to %s' % output)
    else:
        shutil.move(output, prof_file)
        print('Save profile to %s' % prof_file)


def main():
    parser = argparse.ArgumentParser(description='Profile models')
    parser.add_argument('--framework', default='tensorflow',
                        choices=['caffe', 'caffe2',
                                 'tensorflow', 'darknet', 'tf_share'],
                        help='Framework name')
    parser.add_argument('--model', type=str, required=True, help='Model name')
    parser.add_argument('--model_root', type=str, required=True,
                        help='Nexus model root directory')
    parser.add_argument('--version', type=int, default=1,
                        help='Model version (default: 1)')
    parser.add_argument('--gpu_list', required=True,
                        help='GPU indexes. e.g. "0" or "0-2,4,5,7-8".')
    parser.add_argument('--height', type=int, required=True, help='Image height')
    parser.add_argument('--width', type=int, required=True, help='Image width')
    parser.add_argument('--max_batch', type=int, required=True,
                        help='Limit the max batch size')
    parser.add_argument('--min_batch', type=int, default=1,
                        help='Limit the min batch size')
    parser.add_argument('-f', '--force', action='store_true',
                        help='Overwrite the existing model profile in model DB')
    parser.add_argument('--gpu_uuid', action='store_true', default=False,
                        help='Save profile result to a subdirectory with the GPU UUID')
    global args
    args = parser.parse_args()

    profile_model(args)


if __name__ == '__main__':
    main()
