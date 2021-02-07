const CosmosClient = require('@azure/cosmos').CosmosClient;
const { interval,timer } = require('rxjs');
const { exhaustMap } = require('rxjs/operators');

let cosmos = new CosmosClient({
    endpoint: process.env.COSMOS_ENDPOINT,
    key: process.env.COSMOS_KEY
});

let ids = [  ];
let deleted1min = 0;
let queued1min = 0;

interval(60000).pipe(
    exhaustMap( ()=>new Promise(async(resolve)=>{
        console.log(`1 minute performance: Queued:${queued1min} Deleted:${deleted1min} Waiting:${ids.length} Time:${new Date().toISOString().substring(11,19)}`) ;
        queued1min = 0;
        deleted1min = 0;
        resolve();
    }))
).subscribe();

//Deletor daemon
interval(50).pipe(
    exhaustMap(()=>new Promise( async(resolve)=>{
        let deleteResult = null;

        //Always leave the last record as a reference to selector
        while(ids.length > 1){
            deleteResult = null;
            try{
                let record = ids[0];
                deleteResult = await cosmos.database(process.env.COSMOS_DATABASE).container(process.env.COSMOS_CONTAINER).item( record["id"], record[process.env.COSMOS_PARTITIONKEY] ).delete();
                if( deleteResult.statusCode === 204 ){
                    ids.shift();
                    deleted1min ++;
                }
            }catch(exc){
                if( exc.code === 404 ){
                    ids.shift();
                    queued1min --;//double requeue somehow
                }
                else{
                    console.log("Delete exception occured, 1 second cooldown", exc);
                    await timer(1000).toPromise();
                }   
            }
        }
        resolve();
    }))
).subscribe();

//Selector daemon
interval(50).pipe(
    exhaustMap(()=>new Promise( async(resolve)=>{
        //Do not overpopulate queue
        if( ids.length > 500 ){
            resolve();
            return;
        }

        let findResponse;
        try{
            let minId = ids.length > 0 ? ids.pop() : null;
            if( minId != null ) {
                ids.push( minId );
                minId = '"'+minId[ process.env.COSMOS_PARTITIONKEY ]+'"';
            }

            findResponse = await new Promise( resolve2=>   
                cosmos.database(process.env.COSMOS_DATABASE).container(process.env.COSMOS_CONTAINER).items
                .query(process.env.COSMOS_DELETEQUERY + ( minId != null ? " and c."+process.env.COSMOS_PARTITIONKEY+" > " + minId : "" ) )
                .fetchAll()
                .then(resolve2)
                .catch(resolve2)
            );
                
            for( let i = 0 ; i < findResponse.resources.length ; i ++ ){
                ids.push( findResponse.resources[i] );
                queued1min++;
            }
        }
        catch(exc){
            console.log("Select exception occured, 1 second cooldown",findResponse,exc );
            await timer(1000).toPromise();
        }
        resolve();
    }))
).subscribe();