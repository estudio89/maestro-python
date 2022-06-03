import 'package:flutter/material.dart';
import 'package:frontend/repository.dart';
import 'package:frontend/todo_item.dart';
import 'package:uuid/uuid.dart';

import 'models.dart';

void main() {
  runApp(const MyApp());
}

class MyApp extends StatelessWidget {
  const MyApp({Key? key}) : super(key: key);

  // This widget is the root of your application.
  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'Flutter Demo',
      theme: ThemeData(
        // This is the theme of your application.
        //
        // Try running your application with "flutter run". You'll see the
        // application has a blue toolbar. Then, without quitting the app, try
        // changing the primarySwatch below to Colors.green and then invoke
        // "hot reload" (press "r" in the console where you ran "flutter run",
        // or simply save your changes to "hot reload" in a Flutter IDE).
        // Notice that the counter didn't reset back to zero; the application
        // is not restarted.
        primarySwatch: Colors.blue,
      ),
      home: const MyHomePage(title: 'Flutter Demo Home Page'),
    );
  }
}

class MyHomePage extends StatefulWidget {
  const MyHomePage({Key? key, required this.title}) : super(key: key);

  // This widget is the home page of your application. It is stateful, meaning
  // that it has a State object (defined below) that contains fields that affect
  // how it looks.

  // This class is the configuration for the state. It holds the values (in this
  // case the title) provided by the parent (in this case the App widget) and
  // used by the build method of the State. Fields in a Widget subclass are
  // always marked "final".

  final String title;

  @override
  State<MyHomePage> createState() => _MyHomePageState();
}

class _MyHomePageState extends State<MyHomePage> {
  List<Todo> _items = [];

  @override
  void initState() {
    super.initState();
    _refresh();
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      body: Center(
        child: ConstrainedBox(
          constraints: const BoxConstraints(maxWidth: 400),
          child: ListView(
            children: [
              if (_items.isEmpty)
                const Text(
                  "This list looks pretty empty, how about adding an item?",
                  textAlign: TextAlign.center,
                ),
              ..._items
                  .map((item) => TodoItem(
                        item: item,
                        onChange: _onChange,
                        onDelete: _onDelete,
                        key: ValueKey(item.id),
                      ))
                  .toList()
            ],
          ),
        ),
      ),
      floatingActionButton: FloatingActionButton(
        onPressed: _onAdd,
        tooltip: 'Add todo',
        child: const Icon(Icons.add),
      ), // This trailing comma makes auto-formatting nicer for build methods.
    );
  }

  _refresh() async {
    _items = await todoRepository.list();
    setState(() {});
  }

  _onChange(Todo item) {
    print("Change $item");
    setState(() {
      final idx = _items.indexOf(item);
      _items[idx] = item;
    });
    todoRepository.update(item);
  }

  _onDelete(Todo item) {
    print("Delete $item");
    setState(() {
      print("Before $_items");
      _items = _items.where((i) => i.id != item.id).toList();
      print("After $_items");
    });
    todoRepository.delete(item);
  }

  _onAdd() {
    final newItem = Todo(id: const Uuid().v4(), text: "", done: false);
    print("Add $newItem");
    setState(() {
      _items.add(newItem);
    });
    todoRepository.create(newItem);
  }
}
