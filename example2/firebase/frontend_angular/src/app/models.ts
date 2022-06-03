const uuidv4 = () => {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
    var r = (Math.random() * 16) | 0,
      v = c == 'x' ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
};

export class TodoItem {
  id: string;
  text: string;
  done: boolean;

  constructor(id: string | undefined, text: string, done: boolean) {
    if (id) {
      this.id = id;
    } else {
      this.id = uuidv4();
    }
    this.text = text;
    this.done = done;
  }
}
