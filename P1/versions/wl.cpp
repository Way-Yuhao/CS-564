//
// File: wl.cpp
//
//  Description: This program is a word locator written in C++.
//  Student Name: Yuhao Liu
//  UW Campus ID: 9077738517
//  Email: liu697@wisc.edu

#include "wl.h"

using namespace std;
list<string> database;

int readCmd() {
    //read user command
    string cmd;
    getline(cin, cmd);
    //parse command
    string delimiter = " ";
    string token = cmd.substr(0, cmd.find(delimiter));
    //convert to lower case
    transform(token.begin(), token.end(), token.begin(), [](unsigned char c) { return std::tolower(c);});
    if (token == "load") {
        //clear database if necessary
        if (!database.empty())  database.clear();
        //open file
        string filename = cmd.substr(cmd.find(delimiter)+1, cmd.length());
        ifstream ifs;
        ifs.open(filename);
        if (ifs.is_open()) {
            string word;
            char x;
            int index = 0;
            while (!ifs.eof()) {
                x = ifs.get();
                while (isalnum(x) || x == '\'') {
                    word = word + x;
                    x = ifs.get();
                }
                //append to database
                if (word != "") {
                    transform(word.begin(), word.end(), word.begin(), [](unsigned char c) { return std::tolower(c);});
                    database.push_back(word);
                }
                word.clear();
            }
        }
        ifs.close();
        return 0;
    } else if (token == "locate") {
        string key;
        int offset;
        try {
            string cmd_seg = cmd.substr(7, cmd.length());
            key = cmd_seg.substr(0, cmd_seg.find(delimiter));
            transform(key.begin(), key.end(), key.begin(), [](unsigned char c) { return std::tolower(c);});
            string offset_str = cmd_seg.substr(cmd_seg.find(delimiter)+1, cmd_seg.length());
            if (offset_str == "")   throw exception();
            offset = stoi(offset_str);
        } catch (exception e) {
            cout << "ERROR: Invalid command" << endl;
            return 0;
        }
        int index = 1;
        for (list<string>::const_iterator it = database.begin(), end = database.end(); it != end; ++it) {
            if (key == *it) {
                if (offset == 1) {
                    cout << index << endl;
                    return 0;
                } else {
                    offset--;
                }
            }
            index++;
        }
        cout << "No matching entry" << endl;
        return 0;
    } else if (token == "new") {
        if (cmd != "new" && cmd != "new ") {
            cout << "ERROR: Invalid command" << endl;
            return 0;
        }
        //reset the word list
        database.clear();
        return 0;
    } else if (token == "end") {
        if (cmd != "end" && cmd != "end ") {
            cout << "ERROR: Invalid command" << endl;
            return 0;
        }
        return 1;
    } else {
        cout << "ERROR: Invalid command" << endl;
        return 0;
    }
}
int main() {
    int r = 0;
    do {
        cout << ">";
        r = readCmd();
    } while (r != 1);
    return 0;
}

