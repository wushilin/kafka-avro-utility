# Field Spec Configuration

The field spec file allows you to control how field values are generated in your Avro records. By default, the producer uses `field_spec.txt`, but you can specify a different file using the `--field-spec` argument.

## Usage

```bash
rust-avro-producer --field-spec field_spec.txt
```

## File Format

Each line defines a field generator in the format:
```
field_name,template:template_string
```

- Lines starting with `field_name` or empty lines are ignored.
- Everything after `template:` is treated as the template string.
- Placeholders in the template are enclosed in `${...}`.

### Implicit Null Probability
If an Avro field is defined as a **nullable union** (e.g., `["null", "string"]`), there is an implicit **10% chance** that the generated value will be `null`, regardless of the template. The remaining 90% of the time, the template is evaluated.

---

## Generator Functions

You can use these functions inside `${...}` placeholders.

### 1. Sequences & Numbers

| Function | Description | Example |
|----------|-------------|---------|
| `seq(start)` | Incrementing sequence number starting at `start`. | `${seq(1)}` → 1, 2, 3... |
| `range([min, max])` | Random integer between `min` and `max` (inclusive). | `${range([1, 100])}` |
| `random_float([min, max])` | Random float between `min` and `max`. | `${random_float([0.0, 1.0])}` |
| `date_int()` | Days since Unix Epoch (for Avro `date` type). | `${date_int()}` |

### 2. Strings & Data

| Function | Description | Example |
|----------|-------------|---------|
| `random_string([min, max])` | Random alphanumeric string of length `min` to `max`. | `${random_string([5, 10])}` |
| `uuid()` | Random UUID v4 string. | `${uuid()}` |
| `name()` | Random English full name. | `${name()}` |
| `address()` | Random realistic address with street, city, country. | `${address()}` |
| `ipaddr(4)` | Random IPv4 address. | `${ipaddr(4)}` |
| `ipaddr(6)` | Random IPv6 address. | `${ipaddr(6)}` |
| `bytes([len])` | Random ASCII string of length `len` (for `bytes`/`fixed`). | `${bytes([10])}` |
| `fixed([len])` | Alias for `bytes`, useful for `fixed` types. | `${fixed([16])}` |

### 3. Selection

| Function | Description | Example |
|----------|-------------|---------|
| `choice(["a", "b", ...])` | Pick one value uniformly at random. | `${choice(["red", "green", "blue"])}` |
| `choice_weight([...])` | Weighted selection. Pairs of `weight, value`. | `${choice_weight([1, "Rare", 9, "Common"])}` |

### 4. Time

| Function | Description | Example |
|----------|-------------|---------|
| `now_ms()` | Current Unix timestamp in milliseconds. | `${now_ms()}` |
| `now_micros()` | Current Unix timestamp in microseconds. | `${now_micros()}` |
| `now_nanos()` | Current Unix timestamp in nanoseconds. | `${now_nanos()}` |
| `date("format")` | Current date formatted string. | `${date("%Y-%m-%d")}` |
| `date("fmt", "start", "end")` | Random date in range. | `${date("%Y-%m-%d", "2020-01-01", "2023-12-31")}` |

### 5. Formatting

Use `formatted` to generate numbers formatted as strings (e.g., identifiers with leading zeros). It supports Rust-style format syntax: `{: [0] [width] [.precision] }`.

| Syntax | Description | Example Input | Output |
|--------|-------------|---------------|--------|
| `{:04}` | Integer, zero-padded to width 4 | `${formatted(["{:04}", 1, 10])}` | `"0007"` |
| `{:0.4}` | Float, 4 decimal places | `${formatted(["{:0.4}", 0.0, 1.0])}` | `"0.1234"` |
| `{:.2}` | Float, 2 decimal places | `${formatted(["{:.2}", 10.0, 20.0])}` | `"15.50"` |

### 6. JavaScript Integration

You can define custom JavaScript functions at the bottom of your `field_spec.txt` (after a separator line) and invoke them. This allows for complex logic, stateful generation, and cross-field dependencies (via global JS variables).

**Syntax:**
```
field,template:${invokejs(["funcName", arg1, arg2...])}
...
--------------------------
// JavaScript Code
var counter = 0;
function funcName(a, b) {
    return "Computed: " + a + b;
}
```

**Example:**
```
complexField,template:${invokejs(["customGen", "prefix"])}
--------------------------
function customGen(prefix) {
    if (Math.random() > 0.5) {
        return prefix + "-" + Math.floor(Math.random() * 100);
    } else {
        return "fixed-value";
    }
}
```

---

## Union Types

For Avro `Union` types (e.g., `["null", "int", "string"]`), the generator works as follows:

1.  **Implicit Null:** There is a 10% chance the value will be `null`.
2.  **Template Evaluation:** 90% of the time, the template is evaluated to produce a string.
3.  **Type Matching:** The string is parsed against the non-null types in the Union **in order**.
    *   If the string looks like an integer and `int` is the first match, it becomes an `int`.
    *   If it looks like a boolean and `boolean` is available, it becomes a `boolean`.
    *   Otherwise, it falls back to `string`.

**Example:**
If `complexUnion` is `["null", "int", "boolean", "string"]`:
- `${choice(["123", "true", "hello"])}` can generate:
    - `123` (parsed as `int`)
    - `true` (parsed as `boolean`)
    - `"hello"` (parsed as `string`)
    - `null` (randomly)

---

## Example Configuration

```
# Basic Types
id,template:${seq(1)}
age,template:${range([18, 99])}
name,template:${name()}
active,template:${choice(["true", "false"])}

# Formatted IDs
customer_id,template:CUST-${formatted(["{:06}", 1, 999999])}

# Time
created_at,template:${now_ms()}
event_date,template:${date("%Y-%m-%d")}

# Complex Logic via JS
dynamic_val,template:${invokejs(["myFunc"])}

# Weighted Choice
status,template:${choice_weight([10, "OK", 1, "ERROR", 1, "WARNING"])}

# Separator for JS code
--------------------------
function myFunc() {
    return "dynamic-" + Math.random();
}
```
