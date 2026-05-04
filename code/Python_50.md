# Python Hands-On — 50 Questions & Answers
## Focus: List · Set · Dictionary · String
*Predict the output · Fix the bug · Write the code · Real interview style*

---

## 📋 SECTION 1 — LISTS (Q1–Q15)

---

### Q1 (Basic) — What is the output?

```python
nums = [1, 2, 3, 4, 5]
print(nums[1:4])
print(nums[::-1])
print(nums[-2:])
```

**Answer:**
```
[2, 3, 4]
[5, 4, 3, 2, 1]
[4, 5]
```

**Explanation:**
- `nums[1:4]` → index 1 to 3 (4 is excluded) → `[2, 3, 4]`
- `nums[::-1]` → reverse the entire list → `[5, 4, 3, 2, 1]`
- `nums[-2:]` → last 2 elements → `[4, 5]`

---

### Q2 (Basic) — What is the output?

```python
a = [1, 2, 3]
b = a
b.append(4)
print(a)
print(b)
print(a is b)
```

**Answer:**
```
[1, 2, 3, 4]
[1, 2, 3, 4]
True
```

**Explanation:**
`b = a` does NOT copy the list. Both `a` and `b` point to the **same list object** in memory. Any change via `b` also changes `a`. To make a copy use `b = a.copy()` or `b = a[:]`.

---

### Q3 (Basic) — Fix the bug

```python
nums = [3, 1, 4, 1, 5, 9, 2, 6]
nums.sort()
print(nums)

result = nums.sort()
print(result)
```

**What is the bug? What is printed?**

**Answer:**
```
[1, 1, 2, 3, 4, 5, 6, 9]
None
```

**Bug:** `list.sort()` sorts **in-place** and returns `None`. If you need the sorted list in a variable, use `sorted(nums)` instead:
```python
result = sorted(nums)   # returns a new sorted list
print(result)           # [1, 1, 2, 3, 4, 5, 6, 9]
```

---

### Q4 (Medium) — What is the output?

```python
a = [[1, 2], [3, 4]]
b = a.copy()
b[0].append(99)
print(a)
print(b)
```

**Answer:**
```
[[1, 2, 99], [3, 4]]
[[1, 2, 99], [3, 4]]
```

**Explanation:**
`a.copy()` is a **shallow copy** — the outer list is copied, but the inner lists are still shared references. Modifying `b[0]` (which is the same object as `a[0]`) affects both. To avoid this use `copy.deepcopy(a)`.

```python
import copy
b = copy.deepcopy(a)
b[0].append(99)
print(a)   # [[1, 2], [3, 4]]   ← unaffected
print(b)   # [[1, 2, 99], [3, 4]]
```

---

### Q5 (Medium) — Write the code

**Task:** Given a list of numbers, return a new list containing only even numbers, squared.

```python
nums = [1, 2, 3, 4, 5, 6, 7, 8]
# Expected output: [4, 16, 36, 64]
```

**Answer:**
```python
# Method 1 — List comprehension (preferred)
result = [x**2 for x in nums if x % 2 == 0]
print(result)  # [4, 16, 36, 64]

# Method 2 — filter + map (functional style)
result = list(map(lambda x: x**2, filter(lambda x: x % 2 == 0, nums)))
print(result)  # [4, 16, 36, 64]

# Method 3 — traditional loop
result = []
for x in nums:
    if x % 2 == 0:
        result.append(x**2)
print(result)  # [4, 16, 36, 64]
```

---

### Q6 (Medium) — What is the output?

```python
lst = [1, 2, 3, 4, 5]
lst.insert(2, 99)
print(lst)

lst.pop(0)
print(lst)

lst.remove(99)
print(lst)
```

**Answer:**
```
[1, 2, 99, 3, 4, 5]
[2, 99, 3, 4, 5]
[2, 3, 4, 5]
```

**Explanation:**
- `insert(2, 99)` → inserts 99 **at index 2**
- `pop(0)` → removes and returns element **at index 0** (which is 1)
- `remove(99)` → removes the **first occurrence** of value 99

---

### Q7 (Medium) — Write the code

**Task:** Flatten a nested list (one level deep).

```python
nested = [[1, 2, 3], [4, 5], [6, 7, 8, 9]]
# Expected: [1, 2, 3, 4, 5, 6, 7, 8, 9]
```

**Answer:**
```python
# Method 1 — List comprehension (cleanest)
flat = [x for sublist in nested for x in sublist]
print(flat)  # [1, 2, 3, 4, 5, 6, 7, 8, 9]

# Method 2 — sum with empty list
flat = sum(nested, [])
print(flat)  # [1, 2, 3, 4, 5, 6, 7, 8, 9]

# Method 3 — itertools.chain
from itertools import chain
flat = list(chain.from_iterable(nested))
print(flat)  # [1, 2, 3, 4, 5, 6, 7, 8, 9]
```

---

### Q8 (Medium) — What is the output?

```python
a = [1, 2, 3]
b = [4, 5, 6]

print(a + b)
print(a * 2)

a += [10, 11]
print(a)

a.extend([20, 21])
print(a)
```

**Answer:**
```
[1, 2, 3, 4, 5, 6]
[1, 2, 3, 1, 2, 3]
[1, 2, 3, 10, 11]
[1, 2, 3, 10, 11, 20, 21]
```

**Key difference:** `append()` adds one element, `extend()` adds all elements of an iterable.

---

### Q9 (Medium) — Write the code

**Task:** Remove duplicate values from a list while preserving the original order.

```python
lst = [3, 1, 4, 1, 5, 9, 2, 6, 5, 3]
# Expected: [3, 1, 4, 5, 9, 2, 6]
```

**Answer:**
```python
# Method 1 — dict.fromkeys (preserves order, Python 3.7+)
result = list(dict.fromkeys(lst))
print(result)  # [3, 1, 4, 5, 9, 2, 6]

# Method 2 — seen set + loop
seen = set()
result = []
for x in lst:
    if x not in seen:
        result.append(x)
        seen.add(x)
print(result)  # [3, 1, 4, 5, 9, 2, 6]

# Method 3 — set() alone (does NOT preserve order)
result = list(set(lst))
print(result)  # some order, but not guaranteed to match input order
```

---

### Q10 (Advanced) — What is the output?

```python
lst = list(range(10))
print(lst[::2])
print(lst[1::2])
print(lst[2:8:2])
```

**Answer:**
```
[0, 2, 4, 6, 8]
[1, 3, 5, 7, 9]
[2, 4, 6]
```

**Explanation:** Slice syntax is `[start:stop:step]`
- `[::2]` → every 2nd element from start to end
- `[1::2]` → every 2nd element starting from index 1
- `[2:8:2]` → indices 2, 4, 6 (stop=8 excluded)

---

### Q11 (Advanced) — Write the code

**Task:** Given a list of employee dictionaries, sort by salary descending, then by name ascending for ties.

```python
employees = [
    {"name": "Alice", "salary": 90000},
    {"name": "Bob",   "salary": 75000},
    {"name": "Carol", "salary": 90000},
    {"name": "Dave",  "salary": 60000},
]
# Expected: Carol, Alice (both 90k, alphabetical), then Bob, Dave
```

**Answer:**
```python
sorted_emp = sorted(
    employees,
    key=lambda e: (-e["salary"], e["name"])
)
for e in sorted_emp:
    print(e["name"], e["salary"])

# Output:
# Alice 90000
# Carol 90000
# Bob   75000
# Dave  60000
```

**Explanation:** `-e["salary"]` sorts salary in descending order (negate for reverse). `e["name"]` sorts names ascending for ties. Tuple comparison checks left to right.

---

### Q12 (Advanced) — What is the output?

```python
def modify(lst):
    lst.append(4)
    lst = [10, 20, 30]
    lst.append(40)
    print("inside:", lst)

my_list = [1, 2, 3]
modify(my_list)
print("outside:", my_list)
```

**Answer:**
```
inside: [10, 20, 30, 40]
outside: [1, 2, 3, 4]
```

**Explanation:**
- `lst.append(4)` → modifies the ORIGINAL list (passed by reference) → `my_list` becomes `[1, 2, 3, 4]`
- `lst = [10, 20, 30]` → creates a NEW local list, rebinds `lst` to it. Now `lst` no longer points to `my_list`
- Changes after the rebind only affect the local `lst`, not `my_list`

---

### Q13 (Advanced) — Write the code

**Task:** Find the second largest unique element in a list.

```python
nums = [3, 1, 4, 1, 5, 9, 2, 6, 5, 3]
# Expected: 6
```

**Answer:**
```python
# Method 1 — Using sorted + set
unique = sorted(set(nums), reverse=True)
print(unique[1])  # 6

# Method 2 — Using heapq
import heapq
print(heapq.nlargest(2, set(nums))[-1])  # 6

# Method 3 — Single pass (efficient)
first = second = float('-inf')
for n in nums:
    if n > first:
        second = first
        first = n
    elif n > second and n != first:
        second = n
print(second)  # 6
```

---

### Q14 (Advanced) — What is the output?

```python
lst = [1, 2, 3, 4, 5]

for i, val in enumerate(lst):
    if val % 2 == 0:
        lst.remove(val)

print(lst)
```

**Answer:**
```
[1, 3, 5]    # Wait — actually this is WRONG!
```

**Real Answer:**
```
[1, 3, 4, 5]   # ← Unexpected!
```

**Explanation:** Never modify a list while iterating over it. When `val=2` is removed, indices shift — index 2 now holds `4`, but the loop moves to index 2 next, **skipping 3**.

**Fix:**
```python
# Correct approach
lst = [x for x in lst if x % 2 != 0]
print(lst)  # [1, 3, 5]
```

---

### Q15 (Advanced) — Write the code

**Task:** Rotate a list to the right by k positions.

```python
lst = [1, 2, 3, 4, 5]
k = 2
# Expected: [4, 5, 1, 2, 3]
```

**Answer:**
```python
# Method 1 — Slicing (cleanest)
k = k % len(lst)          # handle k > len(lst)
result = lst[-k:] + lst[:-k]
print(result)  # [4, 5, 1, 2, 3]

# Method 2 — Using deque
from collections import deque
d = deque(lst)
d.rotate(k)
print(list(d))  # [4, 5, 1, 2, 3]

# Method 3 — In-place using reversal algorithm
def rotate(lst, k):
    k = k % len(lst)
    lst[:] = lst[::-1]
    lst[:k] = lst[:k][::-1]
    lst[k:] = lst[k:][::-1]
    return lst
print(rotate([1,2,3,4,5], 2))  # [4, 5, 1, 2, 3]
```

---

## 🔵 SECTION 2 — SETS (Q16–Q25)

---

### Q16 (Basic) — What is the output?

```python
s = {1, 2, 3, 2, 1, 4}
print(s)
print(type(s))
print(len(s))
```

**Answer:**
```
{1, 2, 3, 4}
<class 'set'>
4
```

**Explanation:** Sets automatically remove duplicates and are **unordered** — the print order may vary but duplicates are gone.

---

### Q17 (Basic) — What is the output?

```python
a = {1, 2, 3, 4}
b = {3, 4, 5, 6}

print(a | b)    # union
print(a & b)    # intersection
print(a - b)    # difference
print(a ^ b)    # symmetric difference
```

**Answer:**
```
{1, 2, 3, 4, 5, 6}
{3, 4}
{1, 2}
{1, 2, 5, 6}
```

**Explanation:**
- `|` → all elements in either set
- `&` → elements in BOTH sets
- `-` → elements in `a` but NOT in `b`
- `^` → elements in either but NOT in both (union minus intersection)

---

### Q18 (Basic) — Fix the bug

```python
# Goal: create an empty set
s = {}
print(type(s))
s.add(1)
```

**What is the bug?**

**Answer:**
`{}` creates an **empty dictionary**, NOT an empty set. `s.add(1)` will raise `AttributeError: 'dict' object has no attribute 'add'`.

**Fix:**
```python
s = set()        # correct way to create empty set
print(type(s))   # <class 'set'>
s.add(1)         # works fine
print(s)         # {1}
```

---

### Q19 (Medium) — What is the output?

```python
s = {1, 2, 3}
s.add(3)
s.add(4)
s.discard(2)
s.discard(99)   # element not in set
print(s)

# Compare with remove:
try:
    s.remove(99)  # element not in set
except KeyError as e:
    print(f"KeyError: {e}")
```

**Answer:**
```
{1, 3, 4}
KeyError: 99
```

**Explanation:**
- `add(3)` → 3 already exists, set unchanged (no error)
- `discard(99)` → 99 not in set, **no error** raised
- `remove(99)` → 99 not in set, **raises KeyError**

---

### Q20 (Medium) — Write the code

**Task:** Find all common elements between three lists without using loops.

```python
list1 = [1, 2, 3, 4, 5]
list2 = [3, 4, 5, 6, 7]
list3 = [4, 5, 6, 7, 8]
# Expected: {4, 5}
```

**Answer:**
```python
common = set(list1) & set(list2) & set(list3)
print(common)  # {4, 5}

# Alternative using intersection method
common = set(list1).intersection(list2, list3)
print(common)  # {4, 5}
```

---

### Q21 (Medium) — What is the output?

```python
a = {1, 2, 3}
b = {1, 2, 3, 4, 5}

print(a.issubset(b))
print(b.issuperset(a))
print(a.isdisjoint({4, 5, 6}))
print(a.isdisjoint({3, 7, 8}))
```

**Answer:**
```
True
True
True
False
```

**Explanation:**
- `issubset` → every element of `a` is in `b` → True
- `issuperset` → `b` contains all elements of `a` → True
- `isdisjoint({4,5,6})` → no elements in common with `a={1,2,3}` → True
- `isdisjoint({3,7,8})` → 3 is in common → False

---

### Q22 (Medium) — Write the code

**Task:** Given two lists of employee IDs, find employees who are in the first list but NOT in the second (left-only), employees only in the second (right-only), and employees in both.

```python
team_a = [101, 102, 103, 104, 105]
team_b = [103, 104, 105, 106, 107]
```

**Answer:**
```python
a = set(team_a)
b = set(team_b)

only_in_a  = a - b          # {101, 102}
only_in_b  = b - a          # {106, 107}
in_both    = a & b          # {103, 104, 105}
all_unique = a | b          # {101, 102, 103, 104, 105, 106, 107}

print("Only in Team A:", only_in_a)
print("Only in Team B:", only_in_b)
print("In both teams:", in_both)
```

---

### Q23 (Medium) — What is the output?

```python
s = {1, 2, 3, 4, 5}
s.update([6, 7], {8, 9})
print(s)

s2 = frozenset([1, 2, 3])
print(type(s2))
# s2.add(4)   # what would happen?
```

**Answer:**
```
{1, 2, 3, 4, 5, 6, 7, 8, 9}
<class 'frozenset'>
```

**Explanation:**
- `update()` adds multiple iterables at once
- `frozenset` is an immutable version of set — `s2.add(4)` would raise `AttributeError: 'frozenset' object has no attribute 'add'`
- frozensets can be used as dictionary keys or elements of other sets (regular sets cannot)

---

### Q24 (Advanced) — Write the code

**Task:** Given a string, find all characters that appear more than once using sets.

```python
text = "programming"
# Expected: characters that repeat → {'g', 'r', 'm'}
```

**Answer:**
```python
seen = set()
duplicates = set()

for char in text:
    if char in seen:
        duplicates.add(char)
    else:
        seen.add(char)

print(duplicates)  # {'g', 'r', 'm'}

# One-liner using collections
from collections import Counter
duplicates = {ch for ch, count in Counter(text).items() if count > 1}
print(duplicates)  # {'g', 'r', 'm'}
```

---

### Q25 (Advanced) — What is the output and why?

```python
a = {1, 2, 3}
b = {3, 4, 5}

a |= b
print(a)

a &= {1, 2, 3}
print(a)

print({1, 2} < {1, 2, 3})
print({1, 2} < {1, 2})
print({1, 2} <= {1, 2})
```

**Answer:**
```
{1, 2, 3, 4, 5}
{1, 2, 3}
True
False
True
```

**Explanation:**
- `|=` → in-place union (modifies `a`)
- `&=` → in-place intersection
- `<` between sets means **proper subset** (subset but not equal)
- `<=` means **subset or equal**

---

## 🟡 SECTION 3 — DICTIONARIES (Q26–Q38)

---

### Q26 (Basic) — What is the output?

```python
d = {"name": "Alice", "age": 30, "city": "Pune"}

print(d["name"])
print(d.get("salary"))
print(d.get("salary", 0))

d["age"] = 31
d["country"] = "India"
print(d)
```

**Answer:**
```
Alice
None
0
{'name': 'Alice', 'age': 31, 'city': 'Pune', 'country': 'India'}
```

**Key difference:** `d["key"]` raises `KeyError` if key missing. `d.get("key")` returns `None`. `d.get("key", default)` returns the default value.

---

### Q27 (Basic) — What is the output?

```python
d = {"a": 1, "b": 2, "c": 3}

print(list(d.keys()))
print(list(d.values()))
print(list(d.items()))

for key, val in d.items():
    print(f"{key} -> {val}")
```

**Answer:**
```
['a', 'b', 'c']
[1, 2, 3]
[('a', 1), ('b', 2), ('c', 3)]
a -> 1
b -> 2
c -> 3
```

---

### Q28 (Basic) — Fix the bug

```python
# Count frequency of each character in a string
text = "hello world"
freq = {}

for char in text:
    freq[char] += 1    # BUG HERE

print(freq)
```

**Answer:**
The bug is `freq[char] += 1` fails with `KeyError` because `char` doesn't exist in `freq` yet on first occurrence.

```python
# Fix 1 — check before incrementing
freq = {}
for char in text:
    if char in freq:
        freq[char] += 1
    else:
        freq[char] = 1

# Fix 2 — use dict.get()
freq = {}
for char in text:
    freq[char] = freq.get(char, 0) + 1

# Fix 3 — use defaultdict
from collections import defaultdict
freq = defaultdict(int)
for char in text:
    freq[char] += 1

# Fix 4 — best for this case: Counter
from collections import Counter
freq = Counter(text)
print(freq)
# Counter({'l': 3, 'o': 2, 'h': 1, 'e': 1, ' ': 1, 'w': 1, 'r': 1, 'd': 1})
```

---

### Q29 (Medium) — What is the output?

```python
d1 = {"a": 1, "b": 2}
d2 = {"b": 3, "c": 4}

# Merge dictionaries
merged = {**d1, **d2}
print(merged)

d1.update(d2)
print(d1)
```

**Answer:**
```
{'a': 1, 'b': 3, 'c': 4}
{'a': 1, 'b': 3, 'c': 4}
```

**Explanation:** When merging, later keys overwrite earlier ones. `d2["b"] = 3` overwrites `d1["b"] = 2`.

**Python 3.9+ — merge operator:**
```python
merged = d1 | d2     # returns new dict
d1 |= d2             # in-place merge
```

---

### Q30 (Medium) — Write the code

**Task:** Group a list of words by their first letter using a dictionary.

```python
words = ["apple", "banana", "avocado", "blueberry", "cherry", "apricot"]
# Expected: {'a': ['apple', 'avocado', 'apricot'], 'b': ['banana', 'blueberry'], 'c': ['cherry']}
```

**Answer:**
```python
# Method 1 — defaultdict
from collections import defaultdict
grouped = defaultdict(list)
for word in words:
    grouped[word[0]].append(word)
print(dict(grouped))

# Method 2 — setdefault
grouped = {}
for word in words:
    grouped.setdefault(word[0], []).append(word)
print(grouped)

# Method 3 — dict comprehension with sorted
grouped = {
    letter: [w for w in words if w.startswith(letter)]
    for letter in set(w[0] for w in words)
}
print(grouped)
```

---

### Q31 (Medium) — What is the output?

```python
d = {"a": 1, "b": 2, "c": 3, "d": 4}

# Various ways to delete
val = d.pop("a")
print(val, d)

last = d.popitem()
print(last, d)

del d["b"]
print(d)
```

**Answer:**
```
1 {'b': 2, 'c': 3, 'd': 4}
('d', 4) {'b': 2, 'c': 3}
{'c': 3}
```

**Explanation:**
- `pop("a")` → removes key `a` and returns its value
- `popitem()` → removes and returns the **last inserted** key-value pair (Python 3.7+)
- `del d["b"]` → removes key `b`, returns nothing

---

### Q32 (Medium) — Write the code

**Task:** Invert a dictionary (swap keys and values).

```python
original = {"a": 1, "b": 2, "c": 3}
# Expected: {1: "a", 2: "b", 3: "c"}
```

**Answer:**
```python
# Method 1 — dict comprehension
inverted = {v: k for k, v in original.items()}
print(inverted)  # {1: 'a', 2: 'b', 3: 'c'}

# What if values have duplicates?
d = {"a": 1, "b": 2, "c": 1}
inverted = {v: k for k, v in d.items()}
print(inverted)  # {1: 'c', 2: 'b'}  ← 'a' is LOST (c overwrites a for key 1)

# Safe version — group keys with same value
from collections import defaultdict
inverted_safe = defaultdict(list)
for k, v in d.items():
    inverted_safe[v].append(k)
print(dict(inverted_safe))  # {1: ['a', 'c'], 2: ['b']}
```

---

### Q33 (Medium) — What is the output?

```python
d = {}
d["key1"] = [1, 2, 3]
d["key2"] = d["key1"]
d["key2"].append(4)

print(d["key1"])
print(d["key2"])
print(d["key1"] is d["key2"])
```

**Answer:**
```
[1, 2, 3, 4]
[1, 2, 3, 4]
True
```

**Explanation:** `d["key2"] = d["key1"]` makes both keys point to the **same list object**. Mutating via one key affects the other. Use `d["key2"] = d["key1"].copy()` to avoid this.

---

### Q34 (Advanced) — Write the code

**Task:** Find the top 3 most frequent words in a list.

```python
words = ["spark", "python", "spark", "java", "python", "spark",
         "scala", "java", "python", "scala"]
# Expected: [('spark', 3), ('python', 3), ('java', 2)]
```

**Answer:**
```python
# Method 1 — Counter.most_common()
from collections import Counter
freq = Counter(words)
top3 = freq.most_common(3)
print(top3)  # [('spark', 3), ('python', 3), ('java', 2)]

# Method 2 — Manual with sorted
freq = {}
for w in words:
    freq[w] = freq.get(w, 0) + 1

top3 = sorted(freq.items(), key=lambda x: x[1], reverse=True)[:3]
print(top3)  # [('spark', 3), ('python', 3), ('java', 2)]
```

---

### Q35 (Advanced) — What is the output?

```python
# Nested dict access
data = {
    "user": {
        "name": "Alice",
        "scores": [95, 87, 92],
        "address": {
            "city": "Pune",
            "pin": "411001"
        }
    }
}

print(data["user"]["name"])
print(data["user"]["scores"][1])
print(data["user"]["address"]["city"])
print(data.get("user", {}).get("email", "N/A"))
```

**Answer:**
```
Alice
87
Pune
N/A
```

**Explanation:** Chained `.get()` is safe navigation — if "user" doesn't exist, returns `{}`, then `.get("email", "N/A")` on `{}` returns "N/A" without KeyError.

---

### Q36 (Advanced) — Write the code

**Task:** Merge a list of dictionaries, summing values for duplicate keys.

```python
dicts = [
    {"a": 1, "b": 2},
    {"b": 3, "c": 4},
    {"a": 5, "c": 1, "d": 9}
]
# Expected: {"a": 6, "b": 5, "c": 5, "d": 9}
```

**Answer:**
```python
from collections import defaultdict

result = defaultdict(int)
for d in dicts:
    for k, v in d.items():
        result[k] += v

print(dict(result))  # {'a': 6, 'b': 5, 'c': 5, 'd': 9}

# Counter approach
from collections import Counter
result = Counter()
for d in dicts:
    result.update(d)
print(dict(result))  # {'a': 6, 'b': 5, 'c': 5, 'd': 9}
```

---

### Q37 (Advanced) — What is the output?

```python
# Dict comprehension with condition
data = {"alice": 90, "bob": 45, "carol": 78, "dave": 30, "eve": 88}

passed = {name: score for name, score in data.items() if score >= 60}
print(passed)

grades = {
    name: "A" if score >= 90 else "B" if score >= 75 else "C"
    for name, score in data.items()
}
print(grades)
```

**Answer:**
```
{'alice': 90, 'carol': 78, 'eve': 88}
{'alice': 'A', 'bob': 'C', 'carol': 'B', 'dave': 'C', 'eve': 'A'}
```

---

### Q38 (Advanced) — Write the code

**Task:** Given a list of records, create a lookup dictionary keyed by `id`.

```python
records = [
    {"id": 1, "name": "Alice", "dept": "Engineering"},
    {"id": 2, "name": "Bob",   "dept": "Marketing"},
    {"id": 3, "name": "Carol", "dept": "Engineering"},
]
# Expected: {1: {...}, 2: {...}, 3: {...}}
```

**Answer:**
```python
# Method 1 — dict comprehension
lookup = {r["id"]: r for r in records}
print(lookup[1])   # {"id": 1, "name": "Alice", "dept": "Engineering"}
print(lookup[2]["name"])  # Bob

# Method 2 — if you want only specific fields
lookup = {r["id"]: {"name": r["name"], "dept": r["dept"]} for r in records}
print(lookup)
# {1: {'name': 'Alice', 'dept': 'Engineering'},
#  2: {'name': 'Bob', 'dept': 'Marketing'},
#  3: {'name': 'Carol', 'dept': 'Engineering'}}

# Bonus — group by dept
from collections import defaultdict
by_dept = defaultdict(list)
for r in records:
    by_dept[r["dept"]].append(r["name"])
print(dict(by_dept))
# {'Engineering': ['Alice', 'Carol'], 'Marketing': ['Bob']}
```

---

## 🟢 SECTION 4 — STRINGS (Q39–Q50)

---

### Q39 (Basic) — What is the output?

```python
s = "Hello, World!"
print(s.upper())
print(s.lower())
print(s.title())
print(s.replace("World", "Python"))
print(s.startswith("Hello"))
print(s.endswith("!"))
print(len(s))
```

**Answer:**
```
HELLO, WORLD!
hello, world!
Hello, World!
Hello, Python!
True
True
13
```

---

### Q40 (Basic) — What is the output?

```python
s = "  hello world  "
print(s.strip())
print(s.lstrip())
print(s.rstrip())
print(s.strip().split())
print(",".join(["a", "b", "c"]))
```

**Answer:**
```
hello world
hello world  
  hello world
['hello', 'world']
a,b,c
```

---

### Q41 (Basic) — Fix the bug

```python
# Goal: make string uppercase character by character
s = "hello"
for char in s:
    char = char.upper()
print(s)
```

**What is the bug?**

**Answer:**
Strings are **immutable** in Python. Reassigning `char = char.upper()` creates a new string object but does NOT modify `s`. `s` remains `"hello"`.

```python
# Fix 1 — join comprehension
s = "hello"
s = "".join(char.upper() for char in s)
print(s)  # HELLO

# Fix 2 — built-in method
s = "hello".upper()
print(s)  # HELLO
```

---

### Q42 (Medium) — What is the output?

```python
s = "python programming"

print(s.find("pro"))
print(s.find("java"))
print(s.count("p"))
print(s.index("pro"))

try:
    s.index("java")
except ValueError as e:
    print(f"ValueError: {e}")
```

**Answer:**
```
7
-1
2
7
ValueError: substring not found
```

**Key difference:**
- `find()` returns `-1` if not found (safe)
- `index()` raises `ValueError` if not found

---

### Q43 (Medium) — Write the code

**Task:** Check if a string is a palindrome (ignoring spaces and case).

```python
text1 = "A man a plan a canal Panama"
text2 = "Hello World"
# Expected: True, False
```

**Answer:**
```python
def is_palindrome(text):
    cleaned = "".join(text.lower().split())
    return cleaned == cleaned[::-1]

print(is_palindrome("A man a plan a canal Panama"))  # True
print(is_palindrome("Hello World"))                   # False

# One-liner
check = lambda s: (c := "".join(s.lower().split())) == c[::-1]
print(check("racecar"))  # True
```

---

### Q44 (Medium) — What is the output?

```python
# String formatting
name = "Mayuresh"
score = 95.678

print(f"Name: {name}, Score: {score:.2f}")
print(f"Score rounded: {score:.0f}")
print(f"{'hello':>10}")    # right-align in width 10
print(f"{'hello':<10}|")   # left-align in width 10
print(f"{'hello':^10}")    # center in width 10
print(f"{42:05d}")          # zero-padded integer
```

**Answer:**
```
Name: Mayuresh, Score: 95.68
Score rounded: 96
     hello
hello     |
  hello   
00042
```

---

### Q45 (Medium) — Write the code

**Task:** Count vowels and consonants in a string.

```python
text = "Hello World"
# Expected: vowels=3, consonants=7
```

**Answer:**
```python
text = "Hello World"
vowels = set("aeiouAEIOU")

vowel_count = sum(1 for ch in text if ch in vowels)
consonant_count = sum(1 for ch in text if ch.isalpha() and ch not in vowels)

print(f"Vowels: {vowel_count}")        # Vowels: 3
print(f"Consonants: {consonant_count}")  # Consonants: 7

# Using Counter
from collections import Counter
counts = Counter("vowel" if ch in vowels else "consonant"
                 for ch in text if ch.isalpha())
print(counts)  # Counter({'consonant': 7, 'vowel': 3})
```

---

### Q46 (Medium) — What is the output?

```python
s = "data,engineering,is,fun"

parts = s.split(",")
print(parts)
print(len(parts))

# Split with maxsplit
parts2 = s.split(",", 2)
print(parts2)

# rsplit — split from right
parts3 = s.rsplit(",", 1)
print(parts3)
```

**Answer:**
```
['data', 'engineering', 'is', 'fun']
4
['data', 'engineering', 'is,fun']
['data,engineering,is', 'fun']
```

---

### Q47 (Advanced) — Write the code

**Task:** Given a sentence, reverse each word but keep the word order.

```python
text = "Hello World Python"
# Expected: "olleH dlroW nohtyP"
```

**Answer:**
```python
# Method 1 — list comprehension
result = " ".join(word[::-1] for word in text.split())
print(result)  # olleH dlroW nohtyP

# Method 2 — map
result = " ".join(map(lambda w: w[::-1], text.split()))
print(result)  # olleH dlroW nohtyP
```

---

### Q48 (Advanced) — What is the output?

```python
import re

text = "Phone: 9370928372, Alt: 9075935106"

# Find all numbers
numbers = re.findall(r'\d+', text)
print(numbers)

# Replace digits with X
masked = re.sub(r'\d', 'X', text)
print(masked)

# Check if string is all digits
print(re.match(r'^\d+$', "12345") is not None)
print(re.match(r'^\d+$', "123ab") is not None)
```

**Answer:**
```
['9370928372', '9075935106']
Phone: XXXXXXXXXX, Alt: XXXXXXXXXX
True
False
```

---

### Q49 (Advanced) — Write the code

**Task:** Find all duplicate words in a sentence (case-insensitive).

```python
text = "the cat sat on the mat the cat"
# Expected: {'the', 'cat'}
```

**Answer:**
```python
from collections import Counter

words = text.lower().split()
freq = Counter(words)
duplicates = {word for word, count in freq.items() if count > 1}
print(duplicates)  # {'the', 'cat'}

# With counts
print({w: c for w, c in freq.items() if c > 1})
# {'the': 3, 'cat': 2}
```

---

### Q50 (Advanced) — What is the output? Most Tricky!

```python
s1 = "hello"
s2 = "hello"
s3 = "".join(["h", "e", "l", "l", "o"])

print(s1 == s2)       # value equality
print(s1 is s2)       # identity (same object?)
print(s1 is s3)       # identity
print(s1 == s3)       # value equality

# String interning
a = "data_engineer"
b = "data_engineer"
print(a is b)

c = "hello world"
d = "hello world"
print(c is d)         # may vary by implementation!
```

**Answer:**
```
True
True       ← Python interns short simple strings
False      ← s3 is built at runtime, different object
True       ← same VALUE, different OBJECT
True       ← Python interns identifiers
True/False ← implementation-dependent (CPython interns, but not always)
```

**Explanation:**
Python **string interning** — CPython automatically reuses the same object for short strings that look like identifiers (letters, digits, underscore). This is an optimisation.
- `s1 is s2` → True because both refer to the interned `"hello"` object
- `s1 is s3` → False because `s3` is built dynamically at runtime
- **NEVER use `is` to compare string values** — always use `==`

```python
# Golden rule
s1 = "hello"
s2 = input("Enter: ")   # user types "hello"
print(s1 == s2)   # True  ← correct
print(s1 is s2)   # False ← don't use is for values!
```

---

## 🧠 QUICK REFERENCE — Key Methods

### List Methods
```python
lst.append(x)       # add to end
lst.extend(iter)    # add all from iterable
lst.insert(i, x)    # insert at index i
lst.remove(x)       # remove first occurrence of x
lst.pop(i)          # remove & return element at index i
lst.sort()          # sort in-place (returns None)
sorted(lst)         # return new sorted list
lst.reverse()       # reverse in-place
lst.index(x)        # first index of x (ValueError if missing)
lst.count(x)        # count occurrences of x
lst.copy()          # shallow copy
lst.clear()         # remove all elements
```

### Set Methods
```python
s.add(x)            # add element
s.remove(x)         # remove (KeyError if missing)
s.discard(x)        # remove (no error if missing)
s.pop()             # remove & return arbitrary element
s.update(iter)      # add all from iterable
s | t               # union
s & t               # intersection
s - t               # difference
s ^ t               # symmetric difference
s.issubset(t)       # s <= t
s.issuperset(t)     # s >= t
s.isdisjoint(t)     # no common elements
```

### Dictionary Methods
```python
d.get(k)            # value or None
d.get(k, default)   # value or default
d.setdefault(k, v)  # return value, set if key missing
d.pop(k)            # remove and return value (KeyError if missing)
d.pop(k, default)   # remove and return, or return default
d.popitem()         # remove and return last inserted pair
d.update(d2)        # merge d2 into d (d2 values win on conflict)
d.keys()            # view of keys
d.values()          # view of values
d.items()           # view of (key, value) pairs
d.copy()            # shallow copy
d.clear()           # remove all entries
```

### String Methods
```python
s.upper()           # ALL CAPS
s.lower()           # all lowercase
s.title()           # Title Case
s.strip()           # remove leading/trailing whitespace
s.split(sep)        # split into list
sep.join(lst)       # join list into string
s.replace(old, new) # replace all occurrences
s.find(sub)         # index of sub (-1 if missing)
s.index(sub)        # index of sub (ValueError if missing)
s.count(sub)        # count non-overlapping occurrences
s.startswith(pre)   # boolean
s.endswith(suf)     # boolean
s.isdigit()         # True if all digits
s.isalpha()         # True if all letters
s.isalnum()         # True if letters or digits
s.format(**kwargs)  # string formatting
f"..."              # f-string (preferred)
```

---

*End of document — 50 Python Hands-On Q&A*
*Focus: List · Set · Dictionary · String*
