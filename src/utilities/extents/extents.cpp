//
// Copyright notice:
// Free use is permitted under the guidelines and in accordance with the most current
// version of the MIT License.
// https://www.opensource.org/licenses/MIT
//

#include <iostream>
#include <stdio.h>

#include <mcor/arg_map.h>
#include <mcor/mexception.h>
#include <mcor/strutil.h>

#include <nabu_client/database.h>
#include <nabu_client/iop.h>

int Usage()
{
    printf("Displays data columns and their extents in time\n");
    printf("--url : [127.0.0.1/nabu/v1] set URL\n");
    printf("--db  : set database instance\n");
    return 0;
}


int main(int argc, const char * argv[])
{
	try {

        cor::ArgMap argMap;
        argMap.Parse(argv, argc);

        if (argMap.Find("help"))
        {
            Usage();
            return 0;
        }

        cor::Url url("http://127.0.0.1");
        if (argMap.Find("url"))
        {
            url.Set(argMap.Next());
        }

        std::string instance;
        if (argMap.Find("db"))
        {
            instance = argMap.Next();
        }

        // if no particular instance is given, see if the URL serves only one
        // database, in which case it can be assumed as the target
        if (instance.empty())
        {
            std::set<std::string> dbs;
            nabu::client::GetDatabases(url + "nabu/v1", dbs);
            if (dbs.size() == 1)  // exactly one
                instance = *dbs.begin();
            else
            {
                return Usage();
            }
            printf("Assuming database instance '%s'\n", instance.c_str());
        }

        nabu::client::Database database(url + "/nabu/v1", instance);

        // find all columns that will be deleted (at least partly) based
        // on possible wildcard substitution
        std::vector<nabu::MetaKey> allColumns;
        database.GetMain()->GetColumns(nabu::MetaKey(), allColumns);
        std::map<nabu::MetaKey, cor::TimeRange> extentsMap;
        for (size_t i = 0; i < allColumns.size(); i++)
        {
            cor::TimeRange tr = database.GetMain()->Extents(allColumns[i], nabu::Branch::eReadable);
            printf("%-60s %s\n", allColumns[i].GetLiteral().c_str(), tr.Print().c_str());
        }


    } catch (const cor::Exception& err) {
        std::cout << "cor Error: " << err.what() << std::endl;
    } catch (const std::exception& err) {
        std::cout << "std Error: " << err.what() << std::endl;
    }

    return 0;
}

