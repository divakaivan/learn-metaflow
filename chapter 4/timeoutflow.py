from metaflow import FlowSpec, timeout, step, retry
import time
 
class TimeoutFlow(FlowSpec):
 
    @retry
    @timeout(seconds=5)
    @step
    def start(self):
        for i in range(int(time.time() % 10)):
            print(i)    #B
            time.sleep(1)    #B
        self.next(self.end)
 
    @step
    def end(self):
        print('success!')
 
if __name__ == '__main__':
    TimeoutFlow()