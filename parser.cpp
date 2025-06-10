

// the json parser
// for our schema


// #include <boost/pool/pool_alloc.hpp>
// #include <boost/pool/object_pool.hpp>
// #include <boost/polymorphic_memory_resource.hpp>
// #include <boost/container/flat_map.hpp>
// #include <boost/algorithm/string.hpp>
#include <charconv>
#include <string>
#include <string_view>
#include <variant>
#include <vector>
#include <unordered_map>
#include <fstream>
#include <cstring>
#include <iostream>
#include <cassert>
#include <functional>
#include <bits/stdc++.h>


using StringView = std::string_view;
struct Value;
using Object = std::unordered_map<StringView, Value>;
using Array  = std::vector<Value>;

struct Value : std::variant<std::nullptr_t, bool, double, StringView, Array, Object> {
    using variant::variant;
};

// Skip whitespace characters
inline void skipWhitespace(char*& p, char* end) {
    while (p < end && (*p == ' ' || *p == '\n' || *p == '\r' || *p == '\t'))
        ++p;
}

// Forward declarations
Value parseValue(char*& p, char* end);

// Parse a JSON string in-situ, return a string_view into the buffer
StringView parseString(char*& p, char* end) {
    assert(*p == '"');
    char* start = ++p;
    while (p < end && *p != '"') {
        if (*p == '\\' && (p + 1) < end) p += 2;
        else ++p;
    }
    assert(p < end && *p == '"');
    *p = '\0';
    StringView sv(start);
    ++p;
    return sv;
}

// Parse number (integer or floating-point) using from_chars
double parseNumber(char*& p, char* end) {
    double value = 0;
    auto res = std::from_chars(p, end, value);
    assert(res.ec == std::errc());
    p = const_cast<char*>(res.ptr);
    return value;
}

// Parse literals: true, false, null
Value parseLiteral(char*& p, char* end) {
    if (end - p >= 4 && std::strncmp(p, "null", 4) == 0) { p += 4; return nullptr; }
    if (end - p >= 4 && std::strncmp(p, "true", 4) == 0) { p += 4; return true; }
    if (end - p >= 5 && std::strncmp(p, "false", 5) == 0) { p += 5; return false; }
    assert(false && "Invalid literal");
    return nullptr;
}

// Parse JSON array
Array parseArray(char*& p, char* end) {
    assert(*p == '[');
    ++p; skipWhitespace(p, end);
    Array arr;
    if (*p == ']') { ++p; return arr; }
    while (true) {
        arr.push_back(parseValue(p, end));
        skipWhitespace(p, end);
        if (*p == ',') { ++p; skipWhitespace(p, end); continue; }
        break;
    }
    assert(*p == ']'); ++p;
    return arr;
}

// Parse JSON object
Object parseObject(char*& p, char* end) {
    assert(*p == '{');
    ++p; skipWhitespace(p, end);
    Object obj;
    if (*p == '}') { ++p; return obj; }
    while (true) {
        skipWhitespace(p, end);
        StringView key = parseString(p, end);
        skipWhitespace(p, end);
        assert(*p == ':'); ++p; skipWhitespace(p, end);
        Value val = parseValue(p, end);
        obj.emplace(key, std::move(val));
        skipWhitespace(p, end);
        if (*p == ',') { ++p; skipWhitespace(p, end); continue; }
        break;
    }
    assert(*p == '}'); ++p;
    return obj;
}

// Parse any JSON value
Value parseValue(char*& p, char* end) {
    skipWhitespace(p, end);
    assert(p < end);
    switch (*p) {
        case '{': return parseObject(p, end);
        case '[': return parseArray(p, end);
        case '"': return parseString(p, end);
        default:
            if ((*p >= '0' && *p <= '9') || *p == '-') return parseNumber(p, end);
            return parseLiteral(p, end);
    }
}


// Read entire file into a string buffer
std::string readFile(const std::string& filename) {
    std::ifstream in(filename, std::ios::binary);
    if (!in) throw std::runtime_error("Unable to open file");
    in.seekg(0, std::ios::end);
    size_t size = in.tellg();
    in.seekg(0, std::ios::beg);
    std::string buf(size, '\0');
    in.read(buf.data(), size);
    return buf;
}

// to prevent variant errors
std::string valueToString(const Value& v) {
    struct Visitor {
        std::string operator()(std::nullptr_t) const { return "null"; }
        std::string operator()(bool b) const { return b ? "true" : "false"; }
        std::string operator()(double d) const {
            std::ostringstream os; os << d; return os.str();
        }
        std::string operator()(StringView s) const { return std::string(s); }
        std::string operator()(const Array&) const { return "[array]"; }
        std::string operator()(const Object&) const { return "{object}"; }
    };
    return std::visit(Visitor{}, v);
}

// query it now
using Row = Object;
class Query {
    std::vector<Row> rows;
public:
    Query() = default;
    Query(const Array& arr) { for (auto& v: arr) rows.push_back(std::get<Object>(v)); }

    Query(const std::vector<Row>& vec) : rows(vec) {}
    Query filter(std::function<bool(const Row&)> pred) const {
        Query q;
        for (auto& r : rows)
            if (pred(r)) q.rows.push_back(r);
        return q;
    }

    Query project(const std::vector<StringView>& cols) const {
        Query q;
        for (auto& r : rows) {
            Row nr;
            for (auto& c : cols) {
                auto it = r.find(c);
                if (it != r.end()) nr.emplace(it->first, it->second);
            }
            q.rows.push_back(std::move(nr));
        }
        return q;
    }

    // 3) Aggregations Ops
    size_t count() const { return rows.size(); }
    double sum(const StringView& col) const {
        double s = 0;
        for (auto& r : rows) s += std::get<double>(r.at(col));
        return s;
    }
    double average(const StringView& col) const { return rows.empty() ? 0 : sum(col) / rows.size();}
    double min(const StringView& col) const {
        double m = std::numeric_limits<double>::max();
        for (auto& r : rows) m = std::min(m, std::get<double>(r.at(col)));
        return m;
    }
    double max(const StringView& col) const {
        double m = std::numeric_limits<double>::lowest();
        for (auto& r : rows) m = std::max(m, std::get<double>(r.at(col)));
        return m;
    }
    
    // Count how many rows have a column with the given name
    size_t countCol(const StringView& col) const {
        size_t cnt = 0;
        for (const auto& r : rows) {
            if (r.find(col) != r.end()) ++cnt;
        }
        return cnt;
    }



    std::map<std::string, Query> groupBy(const StringView& key) const {
        std::map<std::string, std::vector<Row>> buckets;
        for (auto& r : rows) {
            auto it = r.find(key);
            std::string gk;
            if (it != r.end()) {
                // choosing variant type explicitly
                if (auto p = std::get_if<StringView>(&it->second)) {
                    gk.assign(p->data(), p->size());
                } else if (auto p = std::get_if<double>(&it->second)) {
                    gk = std::to_string(*p);
                } else if (auto p = std::get_if<bool>(&it->second)) {
                    gk = *p ? "true" : "false";
                } else if (std::holds_alternative<std::nullptr_t>(it->second)) {
                    gk = "null";
                }
            }
            buckets[gk].push_back(r);
        }
        std::map<std::string, Query> groups;
        for (auto& [k, vec] : buckets) {
            Query q;
            for (auto& row : vec) q.rows.push_back(row);
            groups.emplace(k, std::move(q));
        }
        return groups;
    }


    // for accessing raw rows
    const std::vector<Row>& data() const { return rows; }
};


int main(int argc, char* argv[]) {
    std::string filedir = "../dataset/tr.json";
    std::string buffer = readFile(filedir);
    char* p   = buffer.data();
    char* end = p + buffer.size();
    Array arr;
    
    // Handle either top-level array or whitespace-separated objects
    if (p<end && *p=='[') {
        Value root = parseValue(p,end);
        auto arrPtr = std::get_if<Array>(&root);
        assert(arrPtr);
        arr = std::move(*arrPtr);
    } else {
        while (true) {
            skipWhitespace(p,end);
            if (p>=end) break;
            Value v = parseValue(p,end);
            assert(std::holds_alternative<Object>(v));
            Object obj = std::move(std::get<Object>(v));
            arr.emplace_back(std::move(obj));
        }
    }


    Query q(arr);
    std::cout << "\nQUERY 1\n\n";
    // --------
    // Query 1
    // --------
    // Overall Record Count
    auto tot = q.count();
    std::cout << "total_trips: " << tot;


    std::cout << "\n\n\n\n";
    std::cout << "QUERY 2\n\n";
    // --------
    // Query 2
    //---------
    // Trip Distance Filter with Payment Type Grouping

    // First we need a query for the trip whose distance more than 5miles
    auto trip_d_5 = q.filter([](auto& r){return std::get<double>(r.at("trip_distance")) > 0.0;});
    // std::cout << trip_d_5.count() << " he\n";

    // Now we group by the type of payment
    auto grp = trip_d_5.groupBy("payment_type");
    // std::cout << grp.size() << std::endl;
    for(auto& [k,sub] : grp) {
        std::cout << "payment_type: " << k  
                  << ", num_trips: " << sub.count() 
                 <<", avg_fare: "<< sub.average("fare_amount") 
                 << ", total_tip: " << sub.sum("tip_amount") << "\n";
    }

    std::cout << "\n\n\n\n" ;
    std::cout << "QUERY 3\n\n";
    // ------ 
    // Query 3
    // ------
    // Store-and-Forward Flag and Date Filter with Vendor Grouping
    
    // this time we have to filter twice before grouping
    auto f1 = q.filter([](auto& r){
        auto flag = std::get<StringView>(r.at("store_and_fwd_flag"));
        auto datetime = std::get<StringView>(r.at("tpep_pickup_datetime"));
        
        // Extract date (YYYY-MM-DD) from 'YYYY-MM-DD hh:mm:ss'
        StringView datePart(datetime.data(), 10);
        return flag == "Y"
            && datePart >= "2024-01-01"
            && datePart <= "2024-01-31";
    });

    auto grup = f1.groupBy("VendorID");
    // std::cout << grp.size() << std::endl;

    for(auto& [k,sub] : grup) {
        // converting key to int, otherwise it would convert to float by default
        int vd = 0;
        auto fc = std::from_chars(k.data(), k.data()+k.size(), vd);
        std::cout << "vendor_id: " << vd 
                  << ", num_trips: " << sub.count() 
                 <<", avg_fare: "<< sub.average("passenger_count") << "\n";
    }


    std::cout  << "\n\n\n\n";
    std::cout << "QUERY 4\n\n";
    // Query 4
    // Daily Statistics for January 2024

    // getting the month
    auto jan2024 = q.filter([](auto& r){
        auto datetime = std::get<StringView>(r.at("tpep_pickup_datetime"));
        
        // Extract date (YYYY-MM-DD) from 'YYYY-MM-DD hh:mm:ss'
        StringView datePart(datetime.data(), 10);
        return datePart >= "2024-01-01"
            && datePart <= "2024-01-31";
    });

    // getting the days
    std::map<std::string, std::vector<Row>> daily;
    for (auto& r : jan2024.data()) {
        auto dt = std::get<StringView>(r.at("tpep_pickup_datetime"));
        StringView datePart(dt.data(), 10);
        daily[std::string(datePart)].push_back(r);
    }
    // now for each day, we'll filter
    for (auto& [date, rowsVec] : daily) {
    Query subq(rowsVec);
    std::cout << "trip_date: " << date
                << ", total_trips: " << subq.count()
                << ", avg_passengers: " << subq.average("passenger_count")
                << ", avg_distance: " << subq.average("trip_distance")
                << ", avg_fare: " << subq.average("fare_amount")
                << ", total_tip: " << subq.sum("tip_amount")
                << "\n";
    }        



    return 0;
}

