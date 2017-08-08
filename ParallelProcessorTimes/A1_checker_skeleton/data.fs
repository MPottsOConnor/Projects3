module Data

open System
open System.IO

let read fname =
    File.ReadAllLines fname 
    |> Array.map (fun line -> line.Trim()) 
    |> Array.filter (fun line -> line.Length > 0 && not (line.StartsWith "//"))
    |> Array.map (fun line -> 
        line.Split (Array.empty<char>, StringSplitOptions.RemoveEmptyEntries)
        |> Array.map (fun s -> Int32.Parse s)
        )
    
let getData fname = 
    try 
        let nums = read fname
        if nums.Length = 0 then failwith "no data"
        if nums.[0].Length <> 2 then failwith "data dimensions?"
        
        let n1, n2 = nums.[0].[0], nums.[0].[1]
        if n1 <= 0 || n2 <= 0 || nums.Length = 1 then failwith "data costs?"
        
        // reshape source s m1 m2? to target t n1 n2 (by extending and/or truncating)
        let s = nums.[1..]
        let m1 = s.Length
        
        let t = Array.zeroCreate<int[]> n1
        
        let min1 = min m1 n1
        for i1 = 0 to min1-1 do
            let si1 = s.[i1]
            let m2 = si1.Length
            let ti1 = Array.zeroCreate<int> n2
            t.[i1] <- ti1
            let min2 = min m2 n2
            for i2 = 0 to min2-1 do
                ti1.[i2] <- si1.[i2]
            for i2 = min2 to n2-1 do
                ti1.[i2] <- ti1.[i2-min2]
                
        for i1 = min1 to n1-1 do
            t.[i1] <- t.[i1-min1] 
        
        t

    with
    | ex -> 
        failwith (sprintf "*** data error: %s" ex.Message)

// ---

let test () =
    let m = getData "m-4-4.txt"
    printfn "%A" m
    
    let m = getData "m-10-10.txt"
    printfn "%A" m
    
    let m = getData "m-3-7.txt"
    printfn "%A" m

    let m = getData "m-err.txt"
    printfn "%A" m

test ()

// ---
