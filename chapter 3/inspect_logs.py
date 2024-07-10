from metaflow import Flow, Run
# for step in Flow("ClassifierTrainFlow").latest_run:
for step in Run('ClassifierTrainFlow/1720610021421620'):
   for task in step:
       if not task.successful:
           print("Task %s failed:" % task.pathspec)
           print("-- Stdout --")
           print(task.stdout)
           print("-- Stderr --")
           print(task.stderr)