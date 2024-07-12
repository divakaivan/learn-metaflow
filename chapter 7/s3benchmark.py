import os
from metaflow import FlowSpec, step, Parameter, S3, profile, parallel_map

URL = 's3://commoncrawl/crawl-data/CC-MAIN-2021-25/segments/1623488519735.70/wet/'
 
def load_s3(s3, num):
    files = list(s3.list_recursive([URL]))[:num]
    total_size = sum(f.size for f in files) / 1024**3
    stats = {}
    with profile('downloading', stats_dict=stats):
        loaded = s3.get_many([f.url for f in files])
    
    s3_gbps = (total_size * 8) / (stats['downloading'] / 1000.)
    print(f'S3->EC2 throughput: {s3_gbps} Gb/s')
    return [obj.path for obj in loaded]

class S3BenchmarkFlow(FlowSpec):
    local_dir = Parameter('local_dir', help='Read local files from this directory')
    num = Parameter('num_files', help='maximum number of files to read', default=50)

    @step
    def start(self):
        with S3() as s3:
            with profile('Loading and processing'):
                if self.local_dir:
                    files = [os.path.join(self.local_dir, f) for f in os.listdir(self.local_dir)][:self.num]
                else:
                    files = load_s3(s3, self.num)
                
                print(f'Reading {len(files)} objects')
                stats = {}
                with profile('reading', stats_dict=stats):
                    size = sum(parallel_map(lambda x: len(open(x, 'rb').read()), files)) / 1024**3
 
                read_gbps = (size * 8) / (stats['reading'] / 1000.)
                print(f'Read {size}GB. Throughput: {read_gbps} Gb/s')
        self.next(self.end)
 
    @step
    def end(self):
        pass
 
if __name__ == '__main__':
    S3BenchmarkFlow()