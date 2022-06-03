import { Injectable } from '@angular/core';
import { TodoItem } from 'src/app/models';

@Injectable({
  providedIn: 'root',
})
export class TodoAPI {
  // private db: firebase.firestore.Firestore;
  private items: TodoItem[] = [];

  constructor() {
    // this.db = firebase.firestore();
  }

  list(): Promise<TodoItem[]> {
    return new Promise<TodoItem[]>((resolve, reject) => {
      resolve(this.items);
    });
    // return this.db
    // .collection('items')
    // .get()
    // .then(snapshot => {
    //     const items: TodoItem[] = [];
    //     snapshot.forEach(doc => {
    //     const item = doc.data() as TodoItem;
    //     item.id = doc.id;
    //     items.push(item);
    //     });
    //     this.items = items;
    //     return items;
    // });
  }

  create(item: TodoItem): Promise<TodoItem> {
    return new Promise<TodoItem>((resolve, reject) => {
      resolve(item);
    });
    // return this.db
    // .collection('items')
    // .add(item)
    // .then(doc => {
    //     item.id = doc.id;
    //     this.items.push(item);
    //     return item;
    // });
  }

  update(item: TodoItem): Promise<TodoItem> {
    return new Promise<TodoItem>((resolve, reject) => {
      resolve(item);
    });
    // return this.db.collection('items').doc(item.id).update(item);
  }

  delete(item: TodoItem): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      resolve();
    });
    // return this.db.collection('items').doc(item.id).delete();
  }
}
