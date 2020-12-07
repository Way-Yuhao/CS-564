//
// File: wl.cpp
//
//  Description: This program is a word locator written in C++.
//  Student Name: Yuhao Liu
//  UW Campus ID: 9077738517
//  Email: liu697@wisc.edu

#include "wl.h"

using namespace std;
list<string> database; //list the stores all words

/**
 * This method reads user command from stdin and runs corresponding executions:
 * 1. load, load given txt docs and store all words in database
 * 2. locate, return the locate on any given word
 * 3. new, clear databse
 * 4. end, exit
 * Otherwise, the method will print out error message
 * @return 1 if end command is entered; 0 otherwise
 */
int readCmd() {
    //read user command
    string cmd;
    getline(cin, cmd);
    //parse command
    string delimiter = " ";
    string token = cmd.substr(0, cmd.find(delimiter));
    //convert to lower case
    transform(token.begin(), token.end(), token.begin(), [](unsigned char c) { return std::tolower(c);});
    //check user command
    if (token == "load") {
        //clear database if necessary
        if (!database.empty())  database.clear();
        //open file
        string filename = cmd.substr(cmd.find(delimiter)+1, cmd.length());
        ifstream ifs; //input file
        ifs.open(filename);
        if (ifs.is_open()) {
            string word;
            char x;
            while (!ifs.eof()) {
                x = ifs.get();
                while (isalnum(x) || x == '\'') {
                    word = word + x;
                    x = ifs.get();
                }
                //append to database
                if (word != "") {
                    transform(word.begin(), word.end(), word.begin(), [](unsigned char c) { return std::tolower(c);});
                    database.push_back(word); //add new word to database
                }
                word.clear();
            }
        } else { // unable to open file
            cout << "ERROR: unable to open file" << endl;
        }
        ifs.close();
        return 0;
    } else if (token == "locate") {
        string key; //the word to find location of
        int offset; //the ith iteration of the word
        try {
            //parse user input
            string cmd_seg = cmd.substr(7, cmd.length());
            key = cmd_seg.substr(0, cmd_seg.find(delimiter));
            transform(key.begin(), key.end(), key.begin(), [](unsigned char c) { return std::tolower(c);});
            string offset_str = cmd_seg.substr(cmd_seg.find(delimiter)+1, cmd_seg.length());
            if (offset_str == "")   throw exception();
            char ch;
            int i = 0;
            for (int i = 0; i < offset_str.length(); i++) {
                ch = offset_str[i];
                if (isspace(ch))
                    throw exception();
            }
            offset = stoi(offset_str);
        } catch (exception e) {
            cout << "ERROR: Invalid command" << endl;
            return 0;
        }
        //locating key word
        int index = 1;
        for (list<string>::const_iterator it = database.begin(), end = database.end(); it != end; ++it) {
            if (key == *it) {
                if (offset == 1) {
                    cout << index << endl; //print out position
                    return 0;
                } else {
                    offset--; //continue looking for the next key word
                }
            }
            index++;
        }
        cout << "No matching entry" << endl;
        return 0;
    } else if (token == "new") { //clear database
        if (cmd != "new" && cmd != "new ") {
            cout << "ERROR: Invalid command" << endl;
            return 0;
        }
        //reset the word list
        database.clear();
        return 0;
    } else if (token == "end") { //exit program
        if (cmd != "end" && cmd != "end ") {
            cout << "ERROR: Invalid command" << endl;
            return 0;
        }
        return 1;
    } else { //print out prompt when an invalid command is entered
        cout << "ERROR: Invalid command" << endl;
        return 0;
    }
}

/**
 * Main method of the program that repeatedly call readCmd(), until the user enters "end"
 * @return 0 when program exits
 */
int main() {
    int r = 0;
    do {
        cout << ">";
        r = readCmd();
    } while (r != 1);
    return 0;
}

