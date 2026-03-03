import pLimit from 'p-limit';
import type { LimitFunction } from 'p-limit';

export class ConcurrentQueue {
  Q: LimitFunction;
  todo = new Map<number, Promise<unknown>>();
  taskId = 0;

  constructor(limit: number) {
    this.Q = pLimit(limit);
  }

  /** Add a task to the queue */
  push(cb: () => Promise<unknown>): void {
    const taskId = this.taskId++;
    this.todo.set(
      taskId,
      this.Q(cb).finally(() => this.todo.delete(taskId)),
    );
  }

  /** Wait for all tasks to finish */
  async join(): Promise<void> {
    while (this.todo.size > 0) await Promise.all(this.todo.values());
  }
}
