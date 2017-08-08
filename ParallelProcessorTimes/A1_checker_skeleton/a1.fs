//mpot023 1020919 ---
open System
open System.Collections.Concurrent
open System.Diagnostics
open System.IO
open System.Linq
open System.Threading
open System.Threading.Tasks
// ---
let PROCS = Environment.ProcessorCount
let tid () = Thread.CurrentThread.ManagedThreadId
let print_procs () = 
    Console.WriteLine (sprintf "... Environment.ProcessorCount: %d" PROCS)

// ---

let duration f = 
    GC.Collect() 
    let timer = new Stopwatch ()
    timer.Start ()
    let res = f ()
    timer.Stop ()
    Console.WriteLine (sprintf "$$$ duration: %i ms" timer.ElapsedMilliseconds)
    res

// ---

let refresh (R:int[][]) =
    for row in R do Array.Clear (row, 0, (Array.length row))

//Print function for 1.1-1.4
let inline print_row_range (R:int[][]) i1 low high =
    let rs = 
        [| for i2 in low .. high-1 -> sprintf "%d" R.[i1 % 2].[i2] |]    
    Console.WriteLine (sprintf "%A %A %s" i1 (low-1) (String.Join (" ", rs)))

//Print function for actors
let inline actorPrint i index low high (R: int[]) =
    let rs = 
        [| for i2 in low .. high -> sprintf "%d" R.[i2]|]
    Console.WriteLine (sprintf "%d %d %s" i index (String.Join(" ", rs)))

//Step function for 1.1-1.4 to calculate max cost using rows[0] and [1]
let inline step (M:int[][]) (R:int[][]) i1 low high =
    if (i1 % 2 = 1) then 
        for i2 = low to high-1 do
            let x = max R.[0].[i2-1] (max R.[0].[i2] R.[0].[i2+1]) 
            R.[1].[i2] <- M.[i1].[i2-1] + x
    else 
        for i2 = low to high-1 do
            let x = max R.[1].[i2-1] (max R.[1].[i2] R.[1].[i2+1])
            R.[0].[i2] <- M.[i1].[i2-1] + x

//Separate step function for actor to prevent complications from using (R[][])
let inline step_actor  (R:int[]) (R':int[]) low high = 
    for i2 = low to high-1 do
           R'.[i2] <- R'.[i2] + max R.[i2-1] (max R.[i2] R.[i2+1])


let mutable Verbose = false

// 1.1 Sequential

let sequential (M:int[][]) (R:int[][]) _ = 
    let N1, N2 = M.Length, M.[0].Length
    Array.blit M.[0] 0 R.[0] 1 N2
    if Verbose then print_row_range R 0 1 (N2+1)
    
    for i1 = 1 to N1-1 do
        step M R i1 1 (N2+1)
        if Verbose then print_row_range R i1 1 (N2+1) 

// 1.2 Parallel Naive

let parallel_naive (M:int[][]) (R:int[][]) _ =
    let N1, N2 = M.Length, M.[0].Length
    Array.blit M.[0] 0 R.[0] 1 N2
    if Verbose then print_row_range R 0 1 (N2+1) //R 0 1 N2
    
    for i1 = 1 to N1-1 do
        if i1 % 2 = 1 then
            Parallel.For (1, N2+1, fun i2 -> 
                let x = max R.[0].[i2-1] (max R.[0].[i2] R.[0].[i2+1])
                R.[1].[i2] <- M.[i1].[i2-1] + x
            ) |> ignore
        else
            Parallel.For (1, N2+1, fun i2 -> 
                let x = max R.[1].[i2-1] (max R.[1].[i2] R.[1].[i2+1])
                R.[0].[i2] <- M.[i1].[i2-1] + x
            ) |> ignore
        if Verbose then print_row_range R i1 1 (N2+1)

// 1.3 Parallel Range

let parallel_range (M:int[][]) (R:int[][]) (W:(int*int)[]) =
    let N1, N2 = M.Length, M.[0].Length
    Array.blit M.[0] 0 R.[0] 1 N2
    if Verbose then print_row_range R 0 1 (N2+1)
    
    let K = W.Length
    for i1 = 1 to N1-1 do
        Parallel.For (0, K, fun j -> 
            let low, high = W.[j]
            step M R i1 low high
            ) |> ignore
        if Verbose then print_row_range R i1 1 (N2+1)
            
// 1.4 Asynchronous Range

let async_range (M:int[][]) (R:int[][]) (W:(int*int)[]) =
    let N1, N2 = M.Length, M.[0].Length
    Array.blit M.[0] 0 R.[0] 1 N2
    if Verbose then print_row_range R 0 1 (N2+1)
    
    let K = W.Length
    for i1 = 1 to N1-1 do
        let asyncs = 
            [| for j in 0..K-1 -> 
                async {
                    let low, high = W.[j]
                    step M R i1 low high
                    } |]
        asyncs |> Async.Parallel |> Async.RunSynchronously |> ignore
        if Verbose then print_row_range R i1 1 (N2+1)


// 1.5 determine side, fifo, put and get functions


type side = 
    | High of int
    | Low of int

let getSide (x: side) =
    match x with
    | High x' -> x'
    | Low x' -> x'

type fifo = list<side> *list<side>

let put_in_high (x: side) (f:fifo) = 
    let f1, f2 = f
    (f1, List.append f2 [x])

let put_in_low (x: side) (f:fifo) = 
    let f1, f2 = f
    (List.append f1 [x], f2)

let get_high (f:fifo) =
    match f with
    | (f1, y::f2) -> y, (f1, f2)
    | _ -> failwith "empty"
    
let get_low (f:fifo) =
    match f with
    | (y::f1, f2) -> y, (f1, f2)
    | _ -> failwith "empty"   

//Determine if the queue is empty
let isempty (f:fifo) =
    match f with
    | (f1,[]) -> true
    | ([],f1) -> true
    | _ -> false

let printfifo (f:fifo) =
    let f1,f2 = f
    printfn "%A %A" f1 f2

// 1.5 Mailbox_range
let mailbox_range (M: int[][]) (W:(int*int)[]) =
    let N1 = M.Length
    let N2 = M.[0].Length
    let k = W.Length
    let counter = ref k

    let actors_started = TaskCompletionSource<bool> ()
    let actors_completed = TaskCompletionSource<bool> ()

    
    let Result = Array.zeroCreate<int> (N2)   
    let actors = Array.zeroCreate<MailboxProcessor<side>> k
    
    //Loop through each actor   
    for j in 0..k-1 do  
        actors.[j] <- MailboxProcessor.Start (fun inbox ->
                if Interlocked.Decrement (counter) = 0 then 
                    counter := k
                    actors_started.SetResult true
                //
                let low, high = W.[j]
                let N2' = high - low
                let low', high' = 1, N2'+1

                let R = Array.zeroCreate<int> (N2'+2) // with sentinel
                let R' = Array.zeroCreate<int> (N2'+2) // with sentinel
                Array.blit M.[0] (low-1) R 1 N2'
                Array.blit M.[1] (low-1) R' 1 N2'
                let actorQueue = ref ([], [])
                let rec loop R R' i =
                    async {                        
                        if i = N1 then
                            Array.blit R 1 Result (low-1) N2'
                            if Interlocked.Decrement (counter) = 0 then 
                                actors_completed.SetResult true
                        else
                            try
                              while (isempty !actorQueue) do
                                  let! m = inbox.Receive ()

                                  match m with
                                  | Low x -> actorQueue := put_in_low m !actorQueue
                                  | High x -> actorQueue := put_in_high m !actorQueue
                                    

                              let (x1':side), (temp: fifo) = get_high !actorQueue       
                              actorQueue := temp 
                              R.[high'] <- getSide x1'

                              let (x2':side), (temp: fifo) = get_low !actorQueue       
                              actorQueue := temp
                              R.[low'-1] <- getSide x2'
                             
                              //print range unimplemented, would ideally be written here but had unresolved type conflicts with pring_range
                              if Verbose then actorPrint (i-1) (N2'*j) low' (high'-1) R
                              step_actor R R' low' (high'-1)                             
                              if Verbose && i+1 = N1 then actorPrint i (N2'*j) low' (high'-1) R'  
                                
                             
                              //Decide whether actor[j] is on the left (low R') or on the right (High R')
                              if j < k-1 then
                                actors.[j+1].Post (Low R'.[high'-1])
                              if j > 0 then
                                actors.[j-1].Post (High R'.[low'])

                              
                              if i + 1 < N1
                                then Array.blit M.[i+1] (low-1) R 1 N2'  
                                 
                              return! (loop R' R (i+1))  
                            with ex -> Console.WriteLine( sprintf "*** %d %d actor exception: %s" j i ex.Message )
                        }

                loop R R' 1
                )

    actors_started.Task.Wait ()

    //Use the post method
    for j2 in 0..k-1 do
       if j2 = k-1 then
          actors.[j2].Post (High 0)
       else 
          actors.[j2].Post (High M.[0].[(N2/k)*(j2+1)])
    
    for j2 in 0..k-1 do
       if j2 = 0 then
          actors.[j2].Post (Low 0)
       else 
          actors.[j2].Post (Low M.[0].[(N2/k)*j2-1])
       
    for i1 in 1..N1-1 do
       actors.[k-1].Post (High 0)
    for i1 in 1..N1-1 do
       actors.[0].Post (Low 0)

    actors_completed.Task.Wait ()
    Result
    |> ignore

// ---
type Msg = 
    | Start
    | FromWest of int
    | FromEast of int

// ---

[<EntryPoint>]
let main args =

    try         
        print_procs ()
        if args.Length = 0 then failwith "no command-line arguments"
        
        //Data
        let fname = args.[0] 
        let alg = if args.Length > 1 then args.[1].ToUpper() else "/*"
        let M = Data.getData fname
        let N1 = Array.length M
        let N2 = Array.length M.[0]                
        
        //Determine Verbose
        let v = args.[3] |> int
        if v = 1 then Verbose <- true else Verbose <- false
        
        //Partitioning
        let k = args.[2] |> int
        let W = if k = 0 then Partitioner.Create(1, N2+1).AsParallel() |> Array.ofSeq |> Array.sort else Partitioner.Create(1, N2+1, k).AsParallel() |> Array.ofSeq |> Array.sort  
                              
        //Set up array of two rows
        let R = Array.init 2 (fun i1 -> Array.create (N2+2) 0)
       
        //Run function for 1.1-1.4
        let run funcname func =
            refresh R
            Console.WriteLine (sprintf "\r\n$$$ %s" funcname)
            duration (fun () -> func M R W) 
            let maxsum = R.[1-N1 % 2].AsParallel().Max()
            Console.WriteLine (sprintf "$$$ %d max_%s" maxsum funcname)

        //Separate run function for actors
        let runActors funcname func = 
            Console.WriteLine(sprintf "\r\n$$$ %s" funcname)
            duration(fun () -> func M W)
        
        match alg with
        
        | "/SEQ" -> run "sequential" sequential;
        | "/PAR-NAIVE" -> run "parallel_naive" parallel_naive;
        | "/PAR-RANGE" -> run "parallel_range" parallel_range;
        | "/ASYNC-RANGE" -> run "async_range" async_range;        
        | "/MAILBOX-RANGE" -> runActors "mailbox_range" mailbox_range;
        | "/*" ->         
            run "sequential" sequential;
            run "parallel_naive" parallel_naive;
            run "parallel_range" parallel_range;
            run "async_range" async_range;
          
            runActors "mailbox_range" mailbox_range;
         | _ -> failwith "Unknown algorithm"
        
        0
        
    with
    | ex -> 
        Console.Error.WriteLine (sprintf "*** %s" ex.Message)
        1
    