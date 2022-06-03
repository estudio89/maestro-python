import { Component } from '@angular/core';
import { TodoAPI } from 'src/app/api';
import { TodoItem } from 'src/app/models';

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.css'],
})
export class AppComponent {
  items: TodoItem[] = [];

  constructor(private api: TodoAPI) {
    this.api.list().then((items: TodoItem[]) => {
      this.items = items;
    });
  }

  onAdd() {
    const newItem = new TodoItem(undefined, '', false);
    this.api.create(newItem);
    this.items.push(newItem);
  }

  trackById(index: number, item: TodoItem) {
    return item.id;
  }

  onChange(item: TodoItem) {
      console.log("change", item);
    this.api.update(item);
  }

  onDelete(item: TodoItem) {
    this.api.delete(item);
    this.items = this.items.filter(i => i.id !== item.id);
  }
}
