#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <parquet/arrow/writer.h>
#include <arrow/io/file.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>

std::shared_ptr<arrow::Table> createArrowTableFromCSV(const std::string& file_path);
void createParquetFile(const std::shared_ptr<arrow::Table>& table, const std::string& file_path);

int main(int argc, char** argv) {
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <input_csv_file> <output_parquet_file>" << std::endl;
        return -1;
    }

    std::string input_csv = argv[1];
    std::string output_parquet = argv[2];

    auto table = createArrowTableFromCSV(input_csv);
    createParquetFile(table, output_parquet);

    return 0;
}

std::shared_ptr<arrow::Table> createArrowTableFromCSV(const std::string& file_path) {
    std::ifstream csv_file(file_path);
    std::string line;
    std::vector<std::shared_ptr<arrow::Field>> fields;
    std::vector<std::shared_ptr<arrow::StringBuilder>> builders;
    std::vector<std::shared_ptr<arrow::Array>> columns;

    // Read the header line to create schema and builders
    if (std::getline(csv_file, line)) {
        std::stringstream ss(line);
        std::string col_name;
        while (std::getline(ss, col_name, ',')) {
            fields.push_back(arrow::field(col_name, arrow::utf8()));
            builders.push_back(std::make_shared<arrow::StringBuilder>());
        }
    }

    auto schema = std::make_shared<arrow::Schema>(fields);

    // Read the rest of the CSV file and populate the builders
    while (std::getline(csv_file, line)) {
        std::stringstream ss(line);
        std::string value;
        size_t i = 0;
        while (std::getline(ss, value, ',')) {
            auto a = builders[i]->Append(value);
            i++;
            std::cout << a << std::endl;
        }
    }

    // Create the Arrow arrays
    for (auto& builder : builders) {
        std::shared_ptr<arrow::Array> array;
        auto b = builder->Finish(&array);
        columns.push_back(array);
        std::cout << b << std::endl;
    }

    return arrow::Table::Make(schema, columns);
}

void createParquetFile(const std::shared_ptr<arrow::Table>& table, const std::string& file_path) {
    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    PARQUET_ASSIGN_OR_THROW(outfile, arrow::io::FileOutputStream::Open(file_path));

    PARQUET_THROW_NOT_OK(
        parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), outfile, 1024)
    );
}