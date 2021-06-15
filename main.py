#!/usr/bin/env python3

import time
import functions as f
from simple_term_menu import TerminalMenu

#CDP6.3.4 kerberos enabled
hbase_thrift_server_host = 'c1677-node4.coelab.cloudera.com'
hbase_thrift_server_port = 9090

def main(client):
    main_menu_title = "Python App - Interact with HBase Thrift Proxy\n"
    main_menu_items = ["[1] Show All Tables", "[2] Create Table", "[3] Enable Table", "[4] Disable Table", "[5] Delete Table","[6] Manipulate Table","[7] Quit"]
    main_menu_cursor = "> "
    main_menu_cursor_style = ("fg_red", "bold")
    main_menu_style = ("standout",)
    main_menu_exit = False

    main_menu = TerminalMenu(
        menu_entries=main_menu_items,
        title=main_menu_title,
        menu_cursor=main_menu_cursor,
        menu_cursor_style=main_menu_cursor_style,
        menu_highlight_style=main_menu_style,
        cycle_cursor=True,
        clear_screen=True,
    )

    manipulatetable_menu_title = "  Manipulate Table\n"
    manipulatetable_menu_items = ["[1] Get Row", "[2] Delete Row", "[3] Mutate Rows","[4] Back to Main Menu"]
    manipulatetable_menu_back = False
    manipulatetable_menu = TerminalMenu(
        manipulatetable_menu_items,
        title=manipulatetable_menu_title,
        menu_cursor=main_menu_cursor,
        menu_cursor_style=main_menu_cursor_style,
        menu_highlight_style=main_menu_style,
        cycle_cursor=True,
        clear_screen=True,
    )

    while not main_menu_exit:
        main_sel = main_menu.show()

        if main_sel == 0:
            print(main_menu_items[0],"\n")
            f.showAllTables(client)
            input("Press Enter to go back...")
            time.sleep(1)
        elif main_sel == 1:
            print(main_menu_items[1])
            f.createTable(client)
            input("Press Enter to go back...")
            time.sleep(1)
        elif main_sel == 2:
            print(main_menu_items[2])
            f.enableTable(client)
            input("Press Enter to go back...")
            time.sleep(1)
        elif main_sel == 3:
            print(main_menu_items[3])
            f.disableTable(client)
            input("Press Enter to go back...")
            time.sleep(1)
        elif main_sel == 4:
            print(main_menu_items[4])
            f.deleteTable(client)
            input("Press Enter to go back...")
            time.sleep(1)
        elif main_sel == 5:
            while not manipulatetable_menu_back:
                manipulatetable_menu_sel = manipulatetable_menu.show()
                if manipulatetable_menu_sel == 0:
                    print(manipulatetable_menu_items[0])
                    f.getRows(client)
                    input("Press Enter to go back...")
                    time.sleep(1)
                elif manipulatetable_menu_sel == 1:
                    print("[2] Delete Row")
                    f.deleteRows(client)
                    input("Press Enter to go back...")
                    time.sleep(1)
                elif manipulatetable_menu_sel == 2:
                    print(manipulatetable_menu_items[2])
                    f.mutateRows(client)
                    input("Press Enter to go back...")
                    time.sleep(1)
                elif manipulatetable_menu_sel == 3:
                    manipulatetable_menu_back = True
                    print("Back to Main Menu")
        elif main_sel == 6:
            main_menu_exit = True
            print("Thanks for playing! Bye!")

if __name__ == "__main__":
    f.kinit()
    f.pbar()
    client,transport=f.connect(hbase_thrift_server_host,hbase_thrift_server_port)
    transport.open()
    main(client)
    transport.close()