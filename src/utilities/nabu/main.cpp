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

#include <nabu/database_imp.h>
#include <nabu_client/database.h>
#include <nabu_client/iop.h>

void
Usage()
{
    printf("--source  <URL>\n");
    printf("     set the URL ('file:\\' or 'http:\\') of the source database\n");
    printf("--dest   <URL>\n");
    printf("     set the URL ('file:\\' or 'http:\\') of the destination database\n");
	printf("--db   <database name>\n");
	printf("     set the name of the database, if more than one exists\n");
    printf("\n");
    printf("--clone\n");
    printf("     make 'dest' a brand-new nabu instance that is a copy of 'source'\n");
    printf("--pull\n");
    printf("     pull from source to dest (source is a server)\n");
    printf("--push\n");
    printf("     push from source to dest (dest is a server)\n");
}


int main(int argc, const char * argv[])
{
	try
	{
	    cor::ArgMap argMap;
	    argMap.Parse(argv, argc);

	    if (argMap.Find("help"))
	    {
	        Usage();
	        return 0;
	    }

	    cor::Url sourceUrl;
	    if (argMap.Find("source"))
	    {
	        sourceUrl.Set(argMap.Next());
	    }
	    else
	    {
	        Usage();
	        return 1;
	    }

	    cor::Url destUrl;
	    if (argMap.Find("dest"))
	    {
	        destUrl.Set(argMap.Next());
	    }
	    else
	    {
	        Usage();
	        return 1;
	    }

	    std::string instance;
	    if (argMap.Find("db"))
	    {
	        instance = argMap.Next();
	    }

	    enum Cmd { ePull, ePush, eClone } cmd;

	    // must have exactly one command
	    if (argMap.Find("pull"))
	        cmd = ePull;
	    else if (argMap.Find("push"))
	        cmd = ePush;
	    else if (argMap.Find("clone"))
	        cmd = eClone;
	    else
	    {
	        Usage();
	        return 1;
	    }

	    nabu::DatabaseImpPtr destImp;
	    nabu::DatabasePtr dest;
	    if (destUrl.GetProtocol() == cor::Url::eFile)
	    {
	        destImp.reset(new nabu::DatabaseImp(destUrl.GetResource()));
	        if (cmd != eClone)
	        {
	            destImp->Initialize();
	        }
	        dest = destImp;
	    }
	    else if (destUrl.GetProtocol() == cor::Url::eHttp ||
                  destUrl.GetProtocol() == cor::Url::eHttps)
	    {
	        // if no particular instance is given, see if the URL serves only one
	        // database, in which case it can be assumed as the target
	        if (instance.empty())
	        {
	            std::set<std::string> dbs;
	            nabu::client::GetDatabases(destUrl + "nabu/v1", dbs);
	            if (dbs.size() == 1)  // exactly one
	                instance = *dbs.begin();
	            else
	            {
	                printf("Need to specify a destination database instance (one of the following):\n");
	                std::set<std::string>::const_iterator i = dbs.begin();
	                for (; i != dbs.end(); i++)
	                    printf("   %s\n", i->c_str());
	                Usage();
	                return 1;
	            }
	            printf("Assuming destination database instance '%s'\n", instance.c_str());
	        }

	        dest.reset(new nabu::client::Database(destUrl + "/nabu/v1", instance));
	    }
	    printf(">>> dest is %s\n", dest->GetLiteral().c_str());

	    nabu::DatabaseImpPtr sourceImp;
	    nabu::DatabasePtr source;
	    if (sourceUrl.GetProtocol() == cor::Url::eFile)
	    {
	        sourceImp.reset(new nabu::DatabaseImp(sourceUrl.GetResource()));
	        sourceImp->Initialize();
	        source = sourceImp;
	    }
	    else if (sourceUrl.GetProtocol() == cor::Url::eHttp ||
                  sourceUrl.GetProtocol() == cor::Url::eHttps)
	    {
	        // if no particular instance is given, see if the URL serves only one
	        // database, in which case it can be assumed as the target
	        if (instance.empty())
	        {
	            std::set<std::string> dbs;
	            nabu::client::GetDatabases(sourceUrl + "nabu/v1", dbs);
	            if (dbs.size() == 1)  // exactly one
	                instance = *dbs.begin();
	            else
	            {
	                printf("Need to specify a source database instance (one of the following):\n");
	                std::set<std::string>::const_iterator i = dbs.begin();
	                for (; i != dbs.end(); i++)
	                    printf("   %s\n", i->c_str());
	                Usage();
	                return 1;
	            }
	            printf("Assuming source database instance '%s'\n", instance.c_str());
	        }

	        source.reset(new nabu::client::Database(sourceUrl + "/nabu/v1", instance));
	    }
	    printf(">>> source is %s\n", source->GetLiteral().c_str());

	    nabu::CopyStats cs;
	    if (cmd == eClone)
	    {
	        if (!destImp)
	            throw cor::Exception("Destination of 'clone' must be a local database");
	        printf("cloning %s -> %s\n", sourceUrl.GetLiteral().c_str(), destUrl.GetLiteral().c_str());
            cs = destImp->Clone(source);
        }
        else if (cmd == ePull)
        {
            if (!destImp)
                throw cor::Exception("Destination of 'pull' must be a local database");
            printf("pulling %s -> %s\n", source->GetLiteral().c_str(), dest->GetLiteral().c_str());
            cs = destImp->Pull(source);
        }
	    else if (cmd == ePush)
	    {
	        if (!sourceImp)
	            throw cor::Exception("Source of 'push' must be a local database");
	        printf("pushing %s -> %s\n", sourceUrl.GetLiteral().c_str(), destUrl.GetLiteral().c_str());
	        cs = sourceImp->Push(dest);
	    }
	    printf("%s\n", cs.Print().c_str());


	    if (sourceImp)
	    {
	        printf("Emptying garbage at %s\n", sourceUrl.GetResource().c_str());
	        sourceImp->GetGarbage().EmptyAllInteractive();
	    }


	    if (destImp)
	    {
	        printf("Emptying garbage at %s\n", destUrl.GetResource().c_str());
	        destImp->GetGarbage().EmptyAllInteractive();
	    }


    } catch (const cor::Exception& err) {
        std::cout << "cor Error: " << err.what() << std::endl;
    } catch (const std::exception& err) {
        std::cout << "std Error: " << err.what() << std::endl;
    }

    return 0;
}

