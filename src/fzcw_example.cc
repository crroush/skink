#include <stdio.h>
#include <spdlog/spdlog.h>
#include <CLI/App.hpp>
#include <CLI/Formatter.hpp>
#include <CLI/Config.hpp>

int main(int argc, char* argv[]){
    CLI::App app{"Simple FCZW"};
    CLI11_PARSE(app, argc, argv);
    spdlog::info( "Initialized" );

}
