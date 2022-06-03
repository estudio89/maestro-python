import {
  Component,
  EventEmitter,
  Input,
  OnDestroy,
  OnInit,
  Output,
} from '@angular/core';
import { Subject, Subscription } from 'rxjs';
import { debounceTime } from 'rxjs/operators';
import { TodoItem } from 'src/app/models';

@Component({
  selector: 'app-todo-item',
  templateUrl: './todo-item.component.html',
  styleUrls: ['./todo-item.component.css'],
})
export class TodoItemComponent implements OnInit, OnDestroy {
  @Input() item: TodoItem;
  @Output() change = new EventEmitter<void>();
  @Output() delete = new EventEmitter<void>();
  debouncerSubscription: Subscription;
  debouncer = new Subject<TodoItem>();

  constructor() {
    this.debouncerSubscription = this.debouncer
      .pipe(debounceTime(500))
      .subscribe((item) => {
        this.change.emit();
      });
  }
  ngOnDestroy(): void {
    this.debouncerSubscription.unsubscribe();
  }

  ngOnInit(): void {}

  onChange() {
    this.debouncer.next(this.item);
  }

  onDelete() {
    this.delete.emit();
  }
}
