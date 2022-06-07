import 'package:cloud_firestore/cloud_firestore.dart';
import 'package:flutter/material.dart';
import 'package:frontend/repository.dart';
import 'package:frontend/todo_item.dart';
import 'package:firebase_core/firebase_core.dart';
import 'package:uuid/uuid.dart';

import 'firebase_options.dart';
import 'models.dart';

void main() async {
  WidgetsFlutterBinding.ensureInitialized();
  await Firebase.initializeApp(
    options: DefaultFirebaseOptions.currentPlatform,
  );
  FirebaseFirestore.instance.useFirestoreEmulator("10.222.0.5", 7070);
  runApp(const MyApp());
}

class MyApp extends StatelessWidget {
  const MyApp({Key? key}) : super(key: key);

  // This widget is the root of your application.
  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      theme: ThemeData(
        primarySwatch: Colors.blue,
        fontFamily: "Ubuntu",
      ),
      home: const MyHomePage(),
    );
  }
}

class MyHomePage extends StatefulWidget {
  const MyHomePage({Key? key}) : super(key: key);

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
          constraints: const BoxConstraints(maxWidth: 500),
          child: ListView(
            children: [
              const SizedBox(height: 16),
              Text(
                "Flutter Web + Cloud Firestore",
                textAlign: TextAlign.center,
                style: TextStyle(
                    fontSize: 32,
                    fontWeight: FontWeight.w600,
                    color: Colors.black.withAlpha(200)),
              ),
              const SizedBox(height: 21),
              if (_items.isEmpty)
                Padding(
                  padding: const EdgeInsets.all(16.0),
                  child: Text(
                    "This list looks pretty empty, how about adding an item?",
                    textAlign: TextAlign.center,
                    style: TextStyle(
                      fontSize: 18,
                      fontWeight: FontWeight.w400,
                      color: Colors.black.withAlpha(200),
                    ),
                  ),
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
      floatingActionButtonLocation: FloatingActionButtonLocation.centerFloat,
      floatingActionButton: FloatingActionButton.large(
        backgroundColor: const Color(0xFF4254B3),
        onPressed: _onAdd,
        tooltip: 'Add todo',
        child: const Icon(
          Icons.add,
          color: Colors.white,
          size: 14,
        ),
      ), // This trailing comma makes auto-formatting nicer for build methods.
    );
  }

  _refresh() async {
    _items = await todoRepository.list();
    setState(() {});
  }

  _onChange(Todo item) {
    setState(() {
      final idx = _items.indexOf(item);
      _items[idx] = item;
    });
    todoRepository.update(item);
  }

  _onDelete(Todo item) {
    setState(() {
      _items = _items.where((i) => i.id != item.id).toList();
    });
    todoRepository.delete(item);
  }

  _onAdd() {
    final newItem = Todo(
        id: const Uuid().v4(), text: "", done: false, date: DateTime.now());
    setState(() {
      _items.add(newItem);
    });
    todoRepository.create(newItem);
  }
}
