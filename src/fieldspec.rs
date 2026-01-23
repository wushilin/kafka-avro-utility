use std::cell::RefCell;
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufRead};
use std::path::Path;
use std::str::FromStr;
use std::sync::atomic::AtomicI64;
use std::sync::Arc;
use std::sync::Mutex;

use chrono::{Datelike, Local, Timelike};
use rquickjs::{Context, Ctx, Function, Result as JsResult, Runtime, Value as JsValue};
use serde_json::Value;
use uuid::Uuid;

// Name lists for the name() function - defined as constants for efficiency
const FIRST_NAMES: &[&str] = &[
    "James",
    "Mary",
    "Robert",
    "Patricia",
    "John",
    "Jennifer",
    "Michael",
    "Linda",
    "David",
    "Elizabeth",
    "William",
    "Barbara",
    "Richard",
    "Susan",
    "Joseph",
    "Jessica",
    "Thomas",
    "Sarah",
    "Charles",
    "Karen",
    "Christopher",
    "Lisa",
    "Daniel",
    "Nancy",
    "Matthew",
    "Betty",
    "Anthony",
    "Margaret",
    "Mark",
    "Sandra",
    "Donald",
    "Ashley",
    "Steven",
    "Kimberly",
    "Paul",
    "Emily",
    "Andrew",
    "Donna",
    "Joshua",
    "Michelle",
    "Kenneth",
    "Carol",
    "Kevin",
    "Amanda",
    "Brian",
    "Melissa",
    "George",
    "Deborah",
    "Timothy",
    "Stephanie",
    "Ronald",
    "Rebecca",
    "Edward",
    "Sharon",
    "Jason",
    "Laura",
    "Jeffrey",
    "Cynthia",
    "Ryan",
    "Dorothy",
    "Jacob",
    "Amy",
    "Gary",
    "Kathleen",
    "Nicholas",
    "Angela",
    "Eric",
    "Shirley",
    "Jonathan",
    "Emma",
    "Stephen",
    "Brenda",
    "Larry",
    "Pamela",
    "Justin",
    "Nicole",
    "Scott",
    "Anna",
    "Brandon",
    "Samantha",
    "Benjamin",
    "Katherine",
    "Samuel",
    "Christine",
    "Gregory",
    "Debra",
    "Alexander",
    "Rachel",
    "Frank",
    "Catherine",
    "Patrick",
    "Carolyn",
    "Raymond",
    "Janet",
    "Jack",
    "Ruth",
    "Dennis",
    "Maria",
    "Jerry",
    "Heather",
    "Tyler",
    "Diane",
    "Aaron",
    "Virginia",
    "Jose",
    "Julie",
    "Adam",
    "Joyce",
    "Henry",
    "Victoria",
    "Nathan",
    "Olivia",
    "Douglas",
    "Kelly",
    "Zachary",
    "Christina",
    "Peter",
    "Lauren",
    "Kyle",
    "Joan",
    "Walter",
    "Evelyn",
    "Ethan",
    "Judith",
    "Jeremy",
    "Megan",
    "Harold",
    "Cheryl",
    "Keith",
    "Andrea",
    "Christian",
    "Hannah",
    "Roger",
    "Martha",
    "Noah",
    "Jacqueline",
    "Gerald",
    "Frances",
    "Carl",
    "Gloria",
    "Terry",
    "Ann",
    "Sean",
    "Teresa",
    "Austin",
    "Kathryn",
    "Arthur",
    "Sara",
    "Lawrence",
    "Janice",
    "Jesse",
    "Jean",
    "Dylan",
    "Alice",
    "Bryan",
    "Madison",
    "Joe",
    "Doris",
    "Jordan",
    "Abigail",
    "Billy",
    "Julia",
    "Bruce",
    "Judy",
    "Albert",
    "Grace",
    "Willie",
    "Denise",
    "Gabriel",
    "Amber",
    "Logan",
    "Marilyn",
    "Alan",
    "Beverly",
    "Juan",
    "Danielle",
    "Wayne",
    "Theresa",
    "Roy",
    "Sophia",
    "Ralph",
    "Marie",
    "Randy",
    "Diana",
    "Eugene",
    "Brittany",
    "Vincent",
    "Natalie",
    "Russell",
    "Isabella",
    "Louis",
    "Charlotte",
    "Philip",
    "Rose",
    "Bobby",
    "Alexis",
    "Johnny",
    "Kayla",
    "Bradley",
    "Lori",
];

const LAST_NAMES: &[&str] = &[
    "Smith",
    "Johnson",
    "Williams",
    "Brown",
    "Jones",
    "Garcia",
    "Miller",
    "Davis",
    "Rodriguez",
    "Martinez",
    "Hernandez",
    "Lopez",
    "Gonzalez",
    "Wilson",
    "Anderson",
    "Thomas",
    "Taylor",
    "Moore",
    "Jackson",
    "Martin",
    "Lee",
    "Perez",
    "Thompson",
    "White",
    "Harris",
    "Sanchez",
    "Clark",
    "Ramirez",
    "Lewis",
    "Robinson",
    "Walker",
    "Young",
    "Allen",
    "King",
    "Wright",
    "Scott",
    "Torres",
    "Nguyen",
    "Hill",
    "Flores",
    "Green",
    "Adams",
    "Nelson",
    "Baker",
    "Hall",
    "Rivera",
    "Campbell",
    "Mitchell",
    "Carter",
    "Roberts",
    "Gomez",
    "Phillips",
    "Evans",
    "Turner",
    "Diaz",
    "Parker",
    "Cruz",
    "Edwards",
    "Collins",
    "Reyes",
    "Stewart",
    "Morris",
    "Morales",
    "Murphy",
    "Cook",
    "Rogers",
    "Gutierrez",
    "Ortiz",
    "Morgan",
    "Cooper",
    "Peterson",
    "Bailey",
    "Reed",
    "Kelly",
    "Howard",
    "Ramos",
    "Kim",
    "Cox",
    "Ward",
    "Richardson",
    "Watson",
    "Brooks",
    "Chavez",
    "Wood",
    "James",
    "Bennett",
    "Gray",
    "Mendoza",
    "Ruiz",
    "Hughes",
    "Price",
    "Alvarez",
    "Castillo",
    "Sanders",
    "Patel",
    "Myers",
    "Long",
    "Ross",
    "Foster",
    "Jimenez",
    "Powell",
    "Jenkins",
    "Perry",
    "Russell",
    "Sullivan",
    "Bell",
    "Coleman",
    "Butler",
    "Henderson",
    "Barnes",
    "Gonzales",
    "Fisher",
    "Vasquez",
    "Simmons",
    "Romero",
    "Jordan",
    "Patterson",
    "Alexander",
    "Hamilton",
    "Graham",
    "Reynolds",
    "Griffin",
    "Wallace",
    "Moreno",
    "West",
    "Cole",
    "Hayes",
    "Bryant",
    "Herrera",
    "Gibson",
    "Ellis",
    "Tran",
    "Medina",
    "Aguilar",
    "Stevens",
    "Murray",
    "Ford",
    "Castro",
    "Marshall",
    "Owens",
    "Harrison",
    "Fernandez",
    "Mcdonald",
    "Woods",
    "Washington",
    "Kennedy",
    "Wells",
    "Aly",
    "Hansen",
    "Wagner",
    "Willis",
    "Olson",
    "Reynolds",
    "Black",
    "Hopkins",
    "Stone",
    "Meyer",
    "Weaver",
    "Webb",
    "Porter",
];

// Address lists
const BUILDING_NAMES: &[&str] = &[
    "The Peak Tower",
    "Empire State Building",
    "Burj Khalifa",
    "Shard",
    "Petronas Towers",
    "Taipei 101",
    "Willis Tower",
    "One World Trade Center",
    "Chrysler Building",
    "Shanghai Tower",
    "Sunshine Apartments",
    "Green Valley Residency",
    "Blue Sky Heights",
    "Golden Gate Towers",
    "Silver Lake Condos",
    "Crystal Palace",
    "Diamond Plaza",
    "Emerald City",
    "Ruby Gardens",
    "Sapphire Suites",
    "Pearl Residency",
    "Opal Court",
    "Topaz Towers",
    "Amethyst Apartments",
];

const STREET_NAMES: &[&str] = &[
    "Parkway Ave",
    "Main St",
    "Broadway",
    "5th Avenue",
    "Wall St",
    "Madison Ave",
    "Sunset Blvd",
    "Hollywood Blvd",
    "Oxford St",
    "Regent St",
    "Baker St",
    "Downing St",
    "Abbey Road",
    "Champs-Elysees",
    "Rodeo Drive",
    "Lombard St",
    "Bourbon St",
    "Beale St",
    "Michigan Ave",
    "Las Vegas Blvd",
    "Ocean Drive",
    "Penny Lane",
    "Wall Street",
    "High Street",
];

const COUNTRY_CITIES: &[(&str, &[&str])] = &[
    (
        "Malaysia",
        &[
            "Kuala Lumpur",
            "George Town",
            "Johor Bahru",
            "Ipoh",
            "Kuching",
        ],
    ),
    (
        "USA",
        &[
            "New York",
            "Los Angeles",
            "Chicago",
            "Houston",
            "Phoenix",
            "San Francisco",
        ],
    ),
    (
        "UK",
        &["London", "Manchester", "Birmingham", "Glasgow", "Liverpool"],
    ),
    ("Japan", &["Tokyo", "Osaka", "Kyoto", "Yokohama", "Sapporo"]),
    (
        "France",
        &["Paris", "Marseille", "Lyon", "Toulouse", "Nice"],
    ),
    ("Singapore", &["Singapore"]),
    (
        "China",
        &["Shanghai", "Beijing", "Guangzhou", "Shenzhen", "Chengdu"],
    ),
    (
        "Australia",
        &["Sydney", "Melbourne", "Brisbane", "Perth", "Adelaide"],
    ),
    ("UAE", &["Dubai", "Abu Dhabi", "Sharjah"]),
    (
        "India",
        &["Mumbai", "Delhi", "Bangalore", "Hyderabad", "Chennai"],
    ),
    ("Thailand", &["Bangkok", "Chiang Mai", "Phuket"]),
    ("South Korea", &["Seoul", "Busan", "Incheon"]),
    ("Indonesia", &["Jakarta", "Surabaya", "Bandung"]),
    ("Turkey", &["Istanbul", "Ankara", "Izmir"]),
    ("Russia", &["Moscow", "Saint Petersburg", "Novosibirsk"]),
    ("Germany", &["Berlin", "Hamburg", "Munich", "Frankfurt"]),
    ("Italy", &["Rome", "Milan", "Naples", "Turin"]),
    ("Spain", &["Madrid", "Barcelona", "Valencia"]),
    ("Canada", &["Toronto", "Montreal", "Vancouver", "Calgary"]),
    ("Brazil", &["Sao Paulo", "Rio de Janeiro", "Brasilia"]),
    ("Egypt", &["Cairo", "Alexandria", "Giza"]),
    ("Nigeria", &["Lagos", "Abuja", "Kano"]),
];

// Thread local storage for JS Runtime/Context
thread_local! {
    static JS_ENGINE: RefCell<Option<(Arc<String>, Runtime, Context)>> = RefCell::new(None);
}

fn parse_weight_expr(v: &Value) -> Option<WeightExpr> {
    // Number -> literal
    if let Some(u) = v.as_u64() {
        return if u > 0 {
            Some(WeightExpr {
                factors: vec![WeightFactor::Literal(u)],
            })
        } else {
            None
        };
    }
    if let Some(i) = v.as_i64() {
        return if i > 0 {
            Some(WeightExpr {
                factors: vec![WeightFactor::Literal(i as u64)],
            })
        } else {
            None
        };
    }
    if let Some(f) = v.as_f64() {
        return if f > 0.0 {
            Some(WeightExpr {
                factors: vec![WeightFactor::Literal(f as u64)],
            })
        } else {
            None
        };
    }

    let s = v.as_str()?;
    let tokens: Vec<&str> = s
        .split(|c| c == 'x' || c == '*' || c == ' ' || c == '\t')
        .filter(|t| !t.is_empty())
        .collect();
    if tokens.is_empty() {
        return None;
    }

    let mut factors = Vec::new();
    for t in tokens {
        let lower = t.to_ascii_lowercase();
        if let Ok(n) = lower.parse::<u64>() {
            if n == 0 {
                return None;
            }
            factors.push(WeightFactor::Literal(n));
            continue;
        }
        let tf = match lower.as_str() {
            "year" => TimeField::Year,
            "month" => TimeField::Month,
            "day" => TimeField::Day,
            "hour" => TimeField::Hour,
            "minute" => TimeField::Minute,
            "second" => TimeField::Second,
            "millisecond" | "ms" => TimeField::Millisecond,
            _ => return None,
        };
        factors.push(WeightFactor::Time(tf));
    }

    if factors.is_empty() {
        None
    } else {
        Some(WeightExpr { factors })
    }
}

fn parse_choice_weight(arg: &Value) -> Option<Placeholder> {
    if let Some(arr) = arg.as_array() {
        if arr.len() >= 2 && arr.len() % 2 == 0 {
            let mut choices = Vec::new();
            let mut weight_exprs = Vec::new();

            for i in (0..arr.len()).step_by(2) {
                if i + 1 >= arr.len() {
                    break;
                }

                let expr = parse_weight_expr(&arr[i])?;
                if expr.eval() == 0 {
                    continue;
                }

                let choice_str = match &arr[i + 1] {
                    Value::String(s) => s.clone(),
                    Value::Number(n) => n.to_string(),
                    Value::Bool(b) => b.to_string(),
                    Value::Null => "null".to_string(),
                    _ => serde_json::to_string(&arr[i + 1]).ok()?,
                };

                weight_exprs.push(expr);
                choices.push(choice_str);
            }

            let cw = ChoiceWeight {
                choices,
                weight_exprs,
            };
            if cw.validate() {
                return Some(Placeholder::ChoiceWeight(cw));
            }
        }
    }
    None
}

fn parse_delta_range(v: &Value) -> Option<(i64, i64)> {
    // number -> single delta
    if let Some(u) = v.as_i64() {
        let d = u.abs().max(1);
        return Some((d, d));
    }
    if let Some(f) = v.as_f64() {
        let d = f.abs().round() as i64;
        let d = d.max(1);
        return Some((d, d));
    }
    let s = v.as_str()?;
    let parts: Vec<&str> = s
        .split("to")
        .flat_map(|p| p.split('-'))
        .map(|p| p.trim())
        .filter(|p| !p.is_empty())
        .collect();
    if parts.len() != 2 {
        return None;
    }
    let a = parts[0].parse::<i64>().ok()?;
    let b = parts[1].parse::<i64>().ok()?;
    let d1 = a.abs().max(1);
    let d2 = b.abs().max(1);
    let lo = d1.min(d2);
    let hi = d1.max(d2);
    Some((lo, hi))
}

fn parse_fluctuate(_name: &str, arg: &Value) -> Option<Placeholder> {
    if let Some(arr) = arg.as_array() {
        if arr.len() == 4 {
            let min = arr[0].as_i64().unwrap_or(0);
            let max = arr[1].as_i64().unwrap_or(0);
            let (delta_min, delta_max) = parse_delta_range(&arr[2])?;
            let unit_str = arr[3].as_str()?;

            let unit = match unit_str.to_ascii_lowercase().as_str() {
                "second" | "seconds" => FluctUnit::Second,
                "minute" | "minutes" => FluctUnit::Minute,
                "hour" | "hours" => FluctUnit::Hour,
                "day" | "days" => FluctUnit::Day,
                _ => return None,
            };

            let key = unit_str.to_string();

            let f = Fluctuate {
                min,
                max,
                delta_min,
                delta_max,
                unit,
                key,
                state: Arc::new(Mutex::new(None)),
            };
            if min <= max {
                return Some(Placeholder::Fluctuate(f));
            }
        }
    }
    None
}
// Optimized function argument structs

pub trait Validatable {
    fn validate(&self) -> bool {
        true
    }
}

pub trait TemplateToken {
    fn to_token(&self) -> String;
}

#[derive(Debug, Clone)]
pub struct ChoiceWeight {
    pub choices: Vec<String>,
    pub weight_exprs: Vec<WeightExpr>,
}

#[derive(Debug, Clone)]
pub enum WeightFactor {
    Literal(u64),
    Time(TimeField),
}

#[derive(Debug, Clone)]
pub enum TimeField {
    Year,
    Month,
    Day,
    Hour,
    Minute,
    Second,
    Millisecond,
}

#[derive(Debug, Clone)]
pub struct WeightExpr {
    pub factors: Vec<WeightFactor>,
}

#[derive(Debug, Clone)]
pub struct Fluctuate {
    pub min: i64,
    pub max: i64,
    pub delta_min: i64,
    pub delta_max: i64,
    pub unit: FluctUnit,
    pub key: String,
    pub state: Arc<Mutex<Option<(String, i64, bool)>>>, // bucket, value, direction_up
}

#[derive(Debug, Clone)]
pub enum FluctUnit {
    Second,
    Minute,
    Hour,
    Day,
}

impl Validatable for ChoiceWeight {
    fn validate(&self) -> bool {
        !self.choices.is_empty() && !self.weight_exprs.is_empty()
    }
}

impl TemplateToken for ChoiceWeight {
    fn to_token(&self) -> String {
        // Evaluate weights at call time to allow dynamic/time-based expressions
        let mut cumulative = Vec::with_capacity(self.weight_exprs.len());
        let mut total = 0u64;
        for expr in &self.weight_exprs {
            let w = expr.eval();
            if w == 0 {
                continue;
            }
            total = total.saturating_add(w);
            cumulative.push(total);
        }
        if total == 0 || cumulative.is_empty() {
            return self.choices.first().cloned().unwrap_or_default();
        }
        let random_weight = rand::random_range(1..=total);
        let idx = cumulative
            .iter()
            .position(|&c| random_weight <= c)
            .unwrap_or(cumulative.len() - 1);
        self.choices[idx].clone()
    }
}

impl TemplateToken for Fluctuate {
    fn to_token(&self) -> String {
        let bucket = current_bucket(self.unit.clone());
        let min = self.min;
        let max = self.max;
        let delta_min = self.delta_min;
        let delta_max = self.delta_max;

        let mut state = self.state.lock().unwrap();
        let entry = state
            .get_or_insert_with(|| (bucket.clone(), rand::random_range(min..=max), true));

        // If still in the same bucket, keep previous value and direction
        if entry.0 == bucket {
            return entry.1.to_string();
        }

        // Bucket changed: optionally flip direction (rare) before stepping
        if entry.1 > min && entry.1 < max && rand::random_bool(0.10) {
            entry.2 = !entry.2;
        }

        entry.1 =
            next_fluct_value_wave(entry.1, min, max, delta_min, delta_max, &mut entry.2);
        entry.0 = bucket;
        entry.1.to_string()
    }
}

fn current_bucket(unit: FluctUnit) -> String {
    let now = Local::now();
    match unit {
        FluctUnit::Second => now.format("%Y-%m-%d-%H-%M-%S").to_string(),
        FluctUnit::Minute => now.format("%Y-%m-%d-%H-%M").to_string(),
        FluctUnit::Hour => now.format("%Y-%m-%d-%H").to_string(),
        FluctUnit::Day => now.format("%Y-%m-%d").to_string(),
    }
}

fn next_fluct_value_wave(
    prev: i64,
    min: i64,
    max: i64,
    delta_min: i64,
    delta_max: i64,
    dir_up: &mut bool,
) -> i64 {
    if min >= max {
        return min;
    }
    let delta_min = delta_min.abs().max(1);
    let delta_max = delta_max.abs().max(delta_min);
    let delta = rand::random_range(delta_min..=delta_max);

    // single-step movement in the chosen direction
    let target = if *dir_up {
        (prev + delta).clamp(min, max)
    } else {
        (prev - delta).clamp(min, max)
    };

    // flip if hit bounds
    if target >= max {
        *dir_up = false;
    } else if target <= min {
        *dir_up = true;
    }

    target
}

impl WeightExpr {
    pub fn eval(&self) -> u64 {
        let now = chrono::Local::now();
        let mut acc: u64 = 1;
        for f in &self.factors {
            let val = match f {
                WeightFactor::Literal(v) => *v,
                WeightFactor::Time(tf) => match tf {
                    TimeField::Year => now.year() as u64,
                    TimeField::Month => now.month() as u64,
                    TimeField::Day => now.day() as u64,
                    TimeField::Hour => now.hour() as u64,
                    TimeField::Minute => now.minute() as u64,
                    TimeField::Second => now.second() as u64,
                    TimeField::Millisecond => (now.timestamp_millis() % 1000) as u64,
                },
            };
            if val == 0 {
                return 0;
            }
            acc = acc.saturating_mul(val);
        }
        acc
    }
}

#[derive(Debug, Clone)]
pub struct Choice {
    pub choices: Vec<String>,
}

impl Validatable for Choice {
    fn validate(&self) -> bool {
        !self.choices.is_empty()
    }
}

impl TemplateToken for Choice {
    fn to_token(&self) -> String {
        let idx = rand::random_range(0..self.choices.len());
        self.choices[idx].clone()
    }
}

#[derive(Debug, Clone)]
pub struct Range {
    pub min: i64,
    pub max: i64,
}

impl Validatable for Range {
    fn validate(&self) -> bool {
        self.min <= self.max
    }
}

impl TemplateToken for Range {
    fn to_token(&self) -> String {
        let val = rand::random_range(self.min..=self.max);
        val.to_string()
    }
}

#[derive(Debug, Clone)]
pub struct RangeFloat {
    pub min: f64,
    pub max: f64,
}

impl Validatable for RangeFloat {
    fn validate(&self) -> bool {
        self.min <= self.max
    }
}

impl TemplateToken for RangeFloat {
    fn to_token(&self) -> String {
        let random_val = rand::random::<f64>() * (self.max - self.min) + self.min;
        random_val.to_string()
    }
}

#[derive(Debug, Clone)]
pub struct RangeString {
    pub min: usize,
    pub max: usize,
}

impl Validatable for RangeString {
    fn validate(&self) -> bool {
        self.min <= self.max
    }
}

impl TemplateToken for RangeString {
    fn to_token(&self) -> String {
        let len = rand::random_range(self.min..=self.max);
        const CHARSET: &[u8] = b"123456789ABCDEFGHIJKLMNPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        (0..len)
            .map(|_| {
                let idx = rand::random_range(0..CHARSET.len());
                CHARSET[idx] as char
            })
            .collect()
    }
}

#[derive(Debug, Clone)]
pub struct DateRange {
    pub format: String,
    pub start_ts: i64,
    pub end_ts: i64,
}

impl Validatable for DateRange {
    fn validate(&self) -> bool {
        self.start_ts <= self.end_ts
    }
}

impl TemplateToken for DateRange {
    fn to_token(&self) -> String {
        let random_ts = rand::random_range(self.start_ts..=self.end_ts);
        if let Some(dt) = chrono::DateTime::from_timestamp_millis(random_ts) {
            dt.format(&self.format).to_string()
        } else {
            "1970-01-01 00:00:00".to_string()
        }
    }
}

#[derive(Debug, Clone)]
pub struct NowMs;

impl TemplateToken for NowMs {
    fn to_token(&self) -> String {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        now.to_string()
    }
}

#[derive(Debug, Clone)]
pub struct NowS;

impl TemplateToken for NowS {
    fn to_token(&self) -> String {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        now.to_string()
    }
}

#[derive(Debug, Clone)]
pub struct NowMicros;

impl TemplateToken for NowMicros {
    fn to_token(&self) -> String {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros();
        let clamped = now.min(i64::MAX as u128);
        clamped.to_string()
    }
}

#[derive(Debug, Clone)]
pub struct NowNanos;

impl TemplateToken for NowNanos {
    fn to_token(&self) -> String {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let clamped = now.min(i64::MAX as u128);
        clamped.to_string()
    }
}

#[derive(Debug, Clone)]
pub struct Name;

impl TemplateToken for Name {
    fn to_token(&self) -> String {
        let first = FIRST_NAMES[rand::random_range(0..FIRST_NAMES.len())];
        let last = LAST_NAMES[rand::random_range(0..LAST_NAMES.len())];
        format!("{} {}", first, last)
    }
}

#[derive(Debug, Clone)]
pub struct IpAddr {
    pub v6: bool,
}

impl TemplateToken for IpAddr {
    fn to_token(&self) -> String {
        if !self.v6 {
            format!(
                "{}.{}.{}.{}",
                rand::random_range(1..255),
                rand::random_range(0..255),
                rand::random_range(0..255),
                rand::random_range(1..255)
            )
        } else {
            format!(
                "{:x}:{:x}:{:x}:{:x}:{:x}:{:x}:{:x}:{:x}",
                rand::random_range(0..0xffff),
                rand::random_range(0..0xffff),
                rand::random_range(0..0xffff),
                rand::random_range(0..0xffff),
                rand::random_range(0..0xffff),
                rand::random_range(0..0xffff),
                rand::random_range(0..0xffff),
                rand::random_range(0..0xffff)
            )
        }
    }
}

#[derive(Debug, Clone)]
pub struct Date {
    pub format: String,
}

impl TemplateToken for Date {
    fn to_token(&self) -> String {
        chrono::Local::now().format(&self.format).to_string()
    }
}

#[derive(Debug, Clone)]
pub struct Address;

impl TemplateToken for Address {
    fn to_token(&self) -> String {
        // 01-42, The Peak Tower, 79 Parkway Ave, Kuala Lumpur, Malaysia 564423
        let level = rand::random_range(1..100);
        let unit = rand::random_range(1..100);
        let building = BUILDING_NAMES[rand::random_range(0..BUILDING_NAMES.len())];
        let number = rand::random_range(1..5001);
        let street = STREET_NAMES[rand::random_range(0..STREET_NAMES.len())];

        // Pick a country first, then a city from that country
        let (country, cities) = COUNTRY_CITIES[rand::random_range(0..COUNTRY_CITIES.len())];
        let city = cities[rand::random_range(0..cities.len())];

        let postal = rand::random_range(10000..999999);

        format!(
            "{:02}-{:02}, {}, {} {}, {}, {} {}",
            level, unit, building, number, street, city, country, postal
        )
    }
}

#[derive(Debug, Clone)]
pub struct Sequence {
    pub counter: Arc<AtomicI64>,
}

impl TemplateToken for Sequence {
    fn to_token(&self) -> String {
        let val = self
            .counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        val.to_string()
    }
}

#[derive(Debug, Clone)]
pub struct UuidGen;

impl TemplateToken for UuidGen {
    fn to_token(&self) -> String {
        Uuid::new_v4().to_string()
    }
}

#[derive(Debug, Clone)]
pub struct DateInt;

impl TemplateToken for DateInt {
    fn to_token(&self) -> String {
        // Days since epoch
        let days = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            / 86400;
        days.to_string()
    }
}

#[derive(Debug, Clone)]
pub struct Bytes {
    pub size: usize,
}

impl Validatable for Bytes {
    fn validate(&self) -> bool {
        self.size > 0
    }
}

impl TemplateToken for Bytes {
    fn to_token(&self) -> String {
        const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        (0..self.size)
            .map(|_| {
                let idx = rand::random_range(0..CHARSET.len());
                CHARSET[idx] as char
            })
            .collect()
    }
}

#[derive(Debug, Clone)]
pub struct FormattedLong {
    pub min: i64,
    pub max: i64,
    pub width: usize,
    pub zero_pad: bool,
}

impl Validatable for FormattedLong {
    fn validate(&self) -> bool {
        self.min <= self.max
    }
}

impl TemplateToken for FormattedLong {
    fn to_token(&self) -> String {
        let val = rand::random_range(self.min..=self.max);
        if self.zero_pad {
            format!("{:0>width$}", val, width = self.width)
        } else {
            format!("{:>width$}", val, width = self.width)
        }
    }
}

#[derive(Debug, Clone)]
pub struct FormattedFloat {
    pub min: f64,
    pub max: f64,
    pub width: usize,
    pub precision: usize,
    pub zero_pad: bool,
}

impl Validatable for FormattedFloat {
    fn validate(&self) -> bool {
        self.min <= self.max
    }
}

impl TemplateToken for FormattedFloat {
    fn to_token(&self) -> String {
        let val = rand::random::<f64>() * (self.max - self.min) + self.min;
        if self.zero_pad {
            format!(
                "{:0>width$.prec$}",
                val,
                width = self.width,
                prec = self.precision
            )
        } else {
            format!(
                "{:>width$.prec$}",
                val,
                width = self.width,
                prec = self.precision
            )
        }
    }
}

#[derive(Debug, Clone)]
pub struct Invoke {
    pub func: String,
    pub args: Vec<Value>,
    pub script: Arc<String>,
}

impl TemplateToken for Invoke {
    fn to_token(&self) -> String {
        JS_ENGINE.with(|engine_cell| {
            let mut engine = engine_cell.borrow_mut();

            let need_init = if let Some((current_script, _, _)) = &*engine {
                !Arc::ptr_eq(current_script, &self.script)
            } else {
                true
            };

            if need_init {
                match Runtime::new() {
                    Ok(rt) => match Context::full(&rt) {
                        Ok(ctx) => {
                            let res = ctx.with(|ctx| ctx.eval::<(), _>(self.script.as_str()));
                            if let Err(e) = res {
                                eprintln!("Error initializing JS context: {}", e);
                            }
                            *engine = Some((self.script.clone(), rt, ctx));
                        }
                        Err(e) => eprintln!("Error creating JS context: {}", e),
                    },
                    Err(e) => eprintln!("Error creating JS runtime: {}", e),
                }
            }

            if let Some((_, _, ctx)) = engine.as_ref() {
                // Execute JS
                let result: JsResult<String> = ctx.with(|ctx| {
                    // Use a trampoline to call the function by name, handling scope issues
                    let trampoline: Function = ctx.eval(format!(
                        "(function(args) {{ return {}.apply(null, args); }})",
                        self.func
                    ))?;

                    let mut js_args = Vec::new();
                    for arg in &self.args {
                        js_args.push(json_to_js(ctx.clone(), arg)?);
                    }

                    let args_array = rquickjs::Array::new(ctx.clone())?;
                    for (i, arg) in js_args.iter().enumerate() {
                        args_array.set(i, arg)?;
                    }

                    let res: JsValue = trampoline.call((args_array,))?;

                    // Convert result to string
                    if res.is_string() {
                        Ok(res.into_string().unwrap().to_string()?)
                    } else if res.is_number() {
                        let string_ctor: Function = ctx.globals().get("String")?;
                        let str_val: rquickjs::String = string_ctor.call((res,))?;
                        Ok(str_val.to_string()?)
                    } else if res.is_bool() {
                        Ok(if res.as_bool().unwrap() {
                            "true".to_string()
                        } else {
                            "false".to_string()
                        })
                    } else {
                        Ok("".to_string())
                    }
                });

                match result {
                    Ok(s) => s,
                    Err(e) => format!("error_invoking_{}:{}", self.func, e),
                }
            } else {
                format!("no_js_context_for_{}", self.func)
            }
        })
    }
}

#[derive(Debug, Clone)]
pub struct GenericFunction {
    pub name: String,
    pub arg: Value,
}

impl TemplateToken for GenericFunction {
    fn to_token(&self) -> String {
        format!("unknown_function_{}", self.name)
    }
}

#[derive(Debug, Clone)]
pub enum Placeholder {
    Sequence(Sequence),
    // Optimized functions
    Choice(Choice),
    ChoiceWeight(ChoiceWeight),
    Fluctuate(Fluctuate),
    Range(Range),
    RangeFloat(RangeFloat),
    RangeString(RangeString),
    DateRange(DateRange),
    // New optimized functions
    NowMs(NowMs),
    NowS(NowS),
    NowMicros(NowMicros),
    NowNanos(NowNanos),
    Name(Name),
    IpAddr(IpAddr),
    Date(Date),
    Address(Address),
    // New functions
    Uuid(UuidGen),
    DateInt(DateInt),
    Bytes(Bytes),
    FormattedLong(FormattedLong),
    FormattedFloat(FormattedFloat),
    Invoke(Invoke),
    // Fallback for functions that don't need optimization
    Generic(GenericFunction),
}

impl TemplateToken for Placeholder {
    fn to_token(&self) -> String {
        match self {
            Placeholder::Sequence(s) => s.to_token(),
            Placeholder::Choice(c) => c.to_token(),
            Placeholder::ChoiceWeight(cw) => cw.to_token(),
            Placeholder::Fluctuate(f) => f.to_token(),
            Placeholder::Range(r) => r.to_token(),
            Placeholder::RangeFloat(r) => r.to_token(),
            Placeholder::RangeString(r) => r.to_token(),
            Placeholder::DateRange(dr) => dr.to_token(),
            Placeholder::NowS(n) => n.to_token(),
            Placeholder::NowMs(n) => n.to_token(),
            Placeholder::NowMicros(n) => n.to_token(),
            Placeholder::NowNanos(n) => n.to_token(),
            Placeholder::Name(n) => n.to_token(),
            Placeholder::IpAddr(i) => i.to_token(),
            Placeholder::Date(d) => d.to_token(),
            Placeholder::Address(a) => a.to_token(),
            Placeholder::Uuid(u) => u.to_token(),
            Placeholder::DateInt(d) => d.to_token(),
            Placeholder::Bytes(b) => b.to_token(),
            Placeholder::FormattedLong(f) => f.to_token(),
            Placeholder::FormattedFloat(f) => f.to_token(),
            Placeholder::Invoke(i) => i.to_token(),
            Placeholder::Generic(g) => g.to_token(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TemplatePart {
    pub text: String,
    pub placeholder: Option<Placeholder>,
}

#[derive(Debug, Clone)]
pub struct Template {
    pub parts: Vec<TemplatePart>,
}

#[derive(Debug, Clone)]
pub enum FieldGenerator {
    Template(Template),
}

#[derive(Debug, Clone)]
pub struct FieldSpec {
    generators: HashMap<String, FieldGenerator>,
}

impl FieldSpec {
    pub fn new() -> Self {
        Self {
            generators: HashMap::new(),
        }
    }

    pub fn load<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let path = path.as_ref();
        if !path.exists() {
            return Ok(Self::new());
        }

        let file = File::open(path)?;
        let reader = io::BufReader::new(file);
        let mut generators = HashMap::new();

        let mut spec_content = String::new();
        let mut script_content = String::new();
        let mut in_script = false;

        for line in reader.lines() {
            let line = line?;
            if !in_script {
                if line.trim().starts_with("----------") && line.trim().len() >= 10 {
                    in_script = true;
                    continue;
                }
                spec_content.push_str(&line);
                spec_content.push('\n');
            } else {
                script_content.push_str(&line);
                script_content.push('\n');
            }
        }

        let script = if script_content.trim().is_empty() {
            None
        } else {
            Some(Arc::new(script_content))
        };

        for (line_num, line) in spec_content.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() || line.starts_with("field_name") {
                continue;
            }

            let parts: Vec<&str> = line.splitn(2, ',').collect();
            if parts.len() != 2 {
                eprintln!(
                    "Warning: Invalid line {} in field spec: {}",
                    line_num + 1,
                    line
                );
                continue;
            }

            let field_name = parts[0].trim().to_string();
            let spec_part = parts[1].trim();

            if spec_part.starts_with("template:") {
                // Parse template with potentially multiple placeholders
                let template_str = &spec_part["template:".len()..];

                let parsed_template = Self::parse_template(template_str, script.clone());
                if let Some(template) = parsed_template {
                    generators.insert(field_name, FieldGenerator::Template(template));
                } else {
                    eprintln!(
                        "Warning: Invalid template format on line {}: {}",
                        line_num + 1,
                        spec_part
                    );
                }
            } else {
                eprintln!(
                    "Warning: Unknown or deprecated spec format on line {}: {}. Only 'template:' is supported.",
                    line_num + 1,
                    spec_part
                );
            }
        }

        Ok(Self { generators })
    }

    /// Extract content between braces, properly handling:
    /// - Quoted strings (single and double quotes)
    /// - Escaped quotes
    /// - Nested braces
    fn extract_braced_content<I>(chars: &mut std::iter::Peekable<I>) -> Option<String>
    where
        I: Iterator<Item = char>,
    {
        let mut inner = String::new();
        let mut brace_depth = 1; // We already consumed the opening {
        let mut in_single_quote = false;
        let mut in_double_quote = false;
        let mut escaped = false;

        while let Some(ch) = chars.next() {
            if escaped {
                // Escaped character - add both backslash and escaped char
                // This preserves escapes for JSON parsing while preventing state changes
                inner.push('\\');
                inner.push(ch);
                escaped = false;
                // Don't process the escaped char as a special character
                continue;
            }

            match ch {
                '\\' => {
                    // Escape character - mark as escaped, will handle on next iteration
                    escaped = true;
                }
                '\'' if !in_double_quote => {
                    // Toggle single quote state (only if not in double quotes)
                    inner.push(ch);
                    in_single_quote = !in_single_quote;
                }
                '"' if !in_single_quote => {
                    // Toggle double quote state (only if not in single quotes)
                    inner.push(ch);
                    in_double_quote = !in_double_quote;
                }
                '{' if !in_single_quote && !in_double_quote => {
                    // Nested opening brace - increase depth (only if not in quotes)
                    inner.push(ch);
                    brace_depth += 1;
                }
                '}' if !in_single_quote && !in_double_quote => {
                    // Closing brace (only if not in quotes)
                    brace_depth -= 1;
                    if brace_depth == 0 {
                        // This is the matching closing brace
                        return Some(inner);
                    } else {
                        // Nested closing brace - still inside
                        inner.push(ch);
                    }
                }
                _ => {
                    // Regular character
                    inner.push(ch);
                }
            }
        }

        // If we ended with an escape (backslash at end of string), preserve it
        if escaped {
            inner.push('\\');
        }

        // Reached end without finding matching brace
        None
    }

    fn parse_template(template_str: &str, script: Option<Arc<String>>) -> Option<Template> {
        let mut parts = Vec::new();
        let mut current_text = String::new();
        let mut chars = template_str.chars().peekable();

        while let Some(ch) = chars.next() {
            if ch == '$' && chars.peek() == Some(&'{') {
                chars.next(); // consume '{'

                // Find the closing } safely, handling quotes and nested braces
                let inner = Self::extract_braced_content(&mut chars)?;

                // Parse the placeholder
                let placeholder = if let Some(open_paren) = inner.find('(') {
                    // Function: ${name(arg)}
                    if inner.ends_with(')') {
                        let name = inner[..open_paren].trim().to_string();
                        let arg_str = &inner[open_paren + 1..inner.len() - 1];

                        if name == "seq" {
                            let start = i64::from_str(arg_str.trim()).ok()?;
                            if start >= 0 {
                                Some(Placeholder::Sequence(Sequence {
                                    counter: Arc::new(AtomicI64::new(start)),
                                }))
                            } else {
                                None
                            }
                        } else {
                            // Try to parse as optimized function, fallback to generic Function
                            let arg = if arg_str.trim().is_empty() {
                                Value::Null
                            } else {
                                serde_json::from_str(arg_str).ok()?
                            };

                            // Try to create optimized placeholder
                            if let Some(placeholder) =
                                Self::parse_optimized_function(&name, &arg, script.clone())
                            {
                                Some(placeholder)
                            } else {
                                // Fallback to generic function
                                Some(Placeholder::Generic(GenericFunction { name, arg }))
                            }
                        }
                    } else {
                        None
                    }
                } else {
                    // No function syntax found - invalid placeholder
                    None
                }?;

                // Add the text part (if any) and the placeholder
                parts.push(TemplatePart {
                    text: current_text.clone(),
                    placeholder: Some(placeholder),
                });
                current_text.clear();
            } else {
                current_text.push(ch);
            }
        }

        // Add any remaining text as the final part
        if !current_text.is_empty() || parts.is_empty() {
            parts.push(TemplatePart {
                text: current_text,
                placeholder: None,
            });
        }

        Some(Template { parts })
    }

    /// Parse function arguments into optimized structs when possible
    fn parse_optimized_function(
        name: &str,
        arg: &Value,
        script: Option<Arc<String>>,
    ) -> Option<Placeholder> {
        match name {
            "choice" => {
                if let Some(arr) = arg.as_array() {
                    let choices: Vec<String> = arr
                        .iter()
                        .filter_map(|v| match v {
                            Value::String(s) => Some(s.clone()),
                            Value::Number(n) => Some(n.to_string()),
                            Value::Bool(b) => Some(b.to_string()),
                            Value::Null => Some("null".to_string()),
                            _ => serde_json::to_string(v).ok(),
                        })
                        .collect();
                    let choice = Choice { choices };
                    if choice.validate() {
                        return Some(Placeholder::Choice(choice));
                    }
                }
                None
            }
            "choice_weight" => parse_choice_weight(arg),
            "fluctuate" => parse_fluctuate(name, arg),
            "range" | "random" => {
                if let Some(arr) = arg.as_array() {
                    if arr.len() == 2 {
                        let min = arr[0].as_i64().unwrap_or(0);
                        let max = arr[1].as_i64().unwrap_or(100);
                        let range = Range { min, max };
                        if range.validate() {
                            return Some(Placeholder::Range(range));
                        }
                    }
                }
                None
            }
            "random_float" => {
                if let Some(arr) = arg.as_array() {
                    if arr.len() == 2 {
                        let min = arr[0].as_f64().unwrap_or(0.0);
                        let max = arr[1].as_f64().unwrap_or(1.0);
                        let range = RangeFloat { min, max };
                        if range.validate() {
                            return Some(Placeholder::RangeFloat(range));
                        }
                    }
                }
                None
            }
            "random_string" => {
                if let Some(arr) = arg.as_array() {
                    if arr.len() == 2 {
                        let min = arr[0].as_u64().unwrap_or(5) as usize;
                        let max = arr[1].as_u64().unwrap_or(10) as usize;
                        let range = RangeString { min, max };
                        if range.validate() {
                            return Some(Placeholder::RangeString(range));
                        }
                    }
                }
                None
            }
            "date" => {
                if let Some(str_arg) = arg.as_str() {
                    return Some(Placeholder::Date(Date {
                        format: str_arg.to_string(),
                    }));
                }
                if let Some(arr) = arg.as_array() {
                    if arr.len() == 3 {
                        let fmt = arr[0].as_str()?;
                        let start_str = arr[1].as_str()?;
                        let end_str = arr[2].as_str()?;

                        let parse_input_date = |date_str: &str| -> Option<i64> {
                            let formats =
                                ["%Y-%m-%d %H:%M:%S%.3f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"];
                            for f in formats {
                                if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(date_str, f) {
                                    return Some(dt.and_utc().timestamp_millis());
                                }
                                if let Ok(date) = chrono::NaiveDate::parse_from_str(date_str, f) {
                                    return Some(
                                        date.and_hms_opt(0, 0, 0)
                                            .unwrap()
                                            .and_utc()
                                            .timestamp_millis(),
                                    );
                                }
                            }
                            None
                        };

                        let start_ts = parse_input_date(start_str)?;
                        let end_ts = parse_input_date(end_str)?;

                        let range = DateRange {
                            format: fmt.to_string(),
                            start_ts,
                            end_ts,
                        };
                        if range.validate() {
                            return Some(Placeholder::DateRange(range));
                        }
                    }
                }
                None
            }
            "now_s" => Some(Placeholder::NowS(NowS)),
            "now_millis" | "now_ms" => Some(Placeholder::NowMs(NowMs)),
            "now_micros" => Some(Placeholder::NowMicros(NowMicros)),
            "now_nanos" => Some(Placeholder::NowNanos(NowNanos)),
            "name" => Some(Placeholder::Name(Name)),
            "address" => Some(Placeholder::Address(Address)),
            "uuid" => Some(Placeholder::Uuid(UuidGen)),
            "date_int" => Some(Placeholder::DateInt(DateInt)),
            "bytes" | "fixed" => {
                if let Some(arr) = arg.as_array() {
                    if !arr.is_empty() {
                        if let Some(size) = arr[0].as_u64() {
                            let bytes = Bytes {
                                size: size as usize,
                            };
                            if bytes.validate() {
                                return Some(Placeholder::Bytes(bytes));
                            }
                        }
                    }
                }
                None
            }
            "formatted" | "formatted_float" => {
                if let Some(arr) = arg.as_array() {
                    if arr.len() == 3 {
                        let fmt_str = arr[0].as_str()?;
                        // Support Rust style format string like {:04} or {:0.4}
                        // Basic parser for subset: {: [0] [width] [.precision] }
                        if !fmt_str.starts_with("{:") || !fmt_str.ends_with('}') {
                            return None;
                        }

                        let inner = &fmt_str[2..fmt_str.len() - 1];
                        let mut chars = inner.chars().peekable();

                        // Optional zero pad
                        let zero_pad = if chars.peek() == Some(&'0') {
                            chars.next();
                            true
                        } else {
                            false
                        };

                        // Optional width
                        let mut width = 0;
                        while let Some(&c) = chars.peek() {
                            if c.is_ascii_digit() {
                                width = width * 10 + (c as usize - '0' as usize);
                                chars.next();
                            } else {
                                break;
                            }
                        }

                        // Optional precision
                        let mut precision = 0;
                        if chars.peek() == Some(&'.') {
                            chars.next();
                            while let Some(&c) = chars.peek() {
                                if c.is_ascii_digit() {
                                    precision = precision * 10 + (c as usize - '0' as usize);
                                    chars.next();
                                } else {
                                    break;
                                }
                            }
                        }

                        // Determine type (float vs int) based on arguments or function name
                        // Check if arguments are floats
                        let is_float_args = arr[1].is_f64() || arr[2].is_f64();
                        let is_float = is_float_args || name == "formatted_float" || precision > 0;

                        if is_float {
                            let min = arr[1]
                                .as_f64()
                                .or_else(|| arr[1].as_i64().map(|i| i as f64))
                                .unwrap_or(0.0);
                            let max = arr[2]
                                .as_f64()
                                .or_else(|| arr[2].as_i64().map(|i| i as f64))
                                .unwrap_or(1.0);

                            let f = FormattedFloat {
                                min,
                                max,
                                width,
                                precision,
                                zero_pad,
                            };
                            if f.validate() {
                                return Some(Placeholder::FormattedFloat(f));
                            }
                        } else {
                            let min = arr[1].as_i64().unwrap_or(0);
                            let max = arr[2].as_i64().unwrap_or(100);

                            let f = FormattedLong {
                                min,
                                max,
                                width,
                                zero_pad,
                            };
                            if f.validate() {
                                return Some(Placeholder::FormattedLong(f));
                            }
                        }
                    }
                }
                None
            }
            "ipaddr" => {
                if let Some(ver) = arg.as_i64() {
                    if ver == 4 {
                        return Some(Placeholder::IpAddr(IpAddr { v6: false }));
                    } else if ver == 6 {
                        return Some(Placeholder::IpAddr(IpAddr { v6: true }));
                    }
                }
                None
            }
            "invokejs" => {
                if let Some(arr) = arg.as_array() {
                    if !arr.is_empty() {
                        if let Some(func_name) = arr[0].as_str() {
                            let args = arr[1..].to_vec();
                            if let Some(script) = script {
                                return Some(Placeholder::Invoke(Invoke {
                                    func: func_name.to_string(),
                                    args,
                                    script: script.clone(),
                                }));
                            }
                        }
                    }
                }
                None
            }
            _ => None,
        }
    }

    pub fn generate_from_template(template: &Template) -> String {
        let mut result = String::new();

        for part in &template.parts {
            result.push_str(&part.text);

            if let Some(placeholder) = &part.placeholder {
                result.push_str(&placeholder.to_token());
            }
        }

        result
    }

    pub fn get_generator(&self, field_name: &str) -> Option<&FieldGenerator> {
        self.generators.get(field_name)
    }
}

fn json_to_js<'js>(ctx: Ctx<'js>, val: &Value) -> JsResult<JsValue<'js>> {
    match val {
        Value::Null => Ok(JsValue::new_null(ctx)),
        Value::Bool(b) => Ok(JsValue::new_bool(ctx, *b)),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                // Use float mostly to avoid i32 truncation in new_int
                Ok(JsValue::new_float(ctx, i as f64))
            } else if let Some(f) = n.as_f64() {
                Ok(JsValue::new_float(ctx, f))
            } else {
                Ok(JsValue::new_int(ctx, 0))
            }
        }
        Value::String(s) => Ok(JsValue::from_string(rquickjs::String::from_str(ctx, s)?)),
        Value::Array(arr) => {
            let js_arr = rquickjs::Array::new(ctx.clone())?;
            for (i, v) in arr.iter().enumerate() {
                js_arr.set(i, json_to_js(ctx.clone(), v)?)?;
            }
            Ok(js_arr.into_value())
        }
        Value::Object(obj) => {
            let js_obj = rquickjs::Object::new(ctx.clone())?;
            for (k, v) in obj {
                js_obj.set(k, json_to_js(ctx.clone(), v)?)?;
            }
            Ok(js_obj.into_value())
        }
    }
}
