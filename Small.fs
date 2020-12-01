// See README.org for a more thorough introduction
module Small

type Key = int
type Value = int

// This is our embedded domain specific language (EDSL). It describes all operations which our little domain supports.
type Operation =
    // We only need Set, but we can simplify other parts by having a larger language.
    // It's a tradeoff. The complexity has to be somewhere.
    | Set of (Key * Value)
    // Set key back to initial value
    | Reset of Key
    // Remove key
    | Remove of Key
    // Increment by one
    | Incr of Key
    // Decrement by one
    | Decr of Key

type State = {
    // Value = (Initial value, Current value)
    Data : Map<Key, (Value * Value)>
    // All operations ever made on our state. Useful to let applications use the interpreter pattern to handle
    // changes
    Audit : Operation list
    // Implementations which interprets the audit needs some state in order to not interpret the same operations
    // twice. This can reside in the state for simple applications like this, or the interpreters can maintain the state
    // somewhere else.
    LastPersisted : Operation
} with
    // It's useful to have a way to construct an empty instance
    static member Empty = {
        Data = Map.empty
        Audit = []
        // Rather than something which can express "empty" like Null Object pattern, null, Option, empty list,
        // we can use an invalid value which can never exist in practice.
        LastPersisted = Remove -1
    }

// It's useful to create helper functions for querying and manipulating state as accessing and mutation patterns
// emerge.

// Our implementation has chosen to be very accepting of errors, and we thus needs some fallback value
// to be used when we don't have data available.
[<Literal>]
let defaultInitial = 1

// Get value or default if the key doesn't exist
let getValue (key : Key) (state : State) : (Value * Value) =
    state.Data
    |> Map.tryFind key
    |> Option.defaultValue (defaultInitial, defaultInitial)

// Set initial and value
let setInitialAndValue (key : Key) (initial : Value) (value : Value) (state : State) : State =
    { state with Data = Map.add key (initial, value) state.Data }
    
// Set only value. Note that we reuse both other functions
let setValue (key : Key) (value : Value) (state : State) : State =
    let initial, _ = getValue key state
    setInitialAndValue key initial value state
    
// Implement our language. Look at the operation, decide what to do, and create a new state with those changes.
// Notice that this doesn't have any side-effects, it only creates new states.
let execute (op : Operation) (state : State) : State =
    match op with
    | Set (key, value) ->
        setValue key value state
    | Reset key ->
        let initial, value = getValue key state
        setInitialAndValue key initial value state
    | Remove key ->
        { state with Data = Map.remove key state.Data }
    | Incr key ->
        let _, value = getValue key state
        setValue key (value + 1) state
    | Decr key ->
        let _, value = getValue key state
        setValue key (value - 1) state
    |> fun state ->
        printfn "Executed %A" op
        { state with Audit = op :: state.Audit }

// Persists to database and mark how for we've interpreted
let persist (state : State) : State =
    state.Audit
    |> Seq.takeWhile (fun op -> not (obj.ReferenceEquals(op, state.LastPersisted)))
    |> Seq.rev
    |> Seq.fold (fun state op ->
        printfn "Saving %A" op
        { state with LastPersisted = op }
    ) state
    
// Something happened in our application, and our state might want to respond to this change.
type ApplicationEvent(key) =
    member val Key = key with get, set
    
// We need functions which maps from events to our language. Your handlers might be simpler if you returned
// Operation list rather than having multiple handlers operating on the same event.
type Handler = State -> ApplicationEvent -> Operation option

// This function will run all handlers sequentially. Depending on your domain, you might want to select handlers
// in parallel, and maybe even run them in parallel, merging the result.
let handle (handlers : Handler list) (ev : ApplicationEvent) (state : State) : State =
    printfn "handle %A" ev
    handlers
    |> Seq.fold (fun state handler ->
        handler state ev
        |> Option.map (fun op -> execute op state)
        |> Option.defaultValue state
    ) state

    
// Then we have events from our actual application

type OrderLineCreated(key) =
    inherit ApplicationEvent(key)
    
type OrderLineWithInitialValueCreated(key, value) =
    inherit ApplicationEvent(key)
    member val Value = value with get,set

type OrderLineRemoved(key) =
    inherit ApplicationEvent(key)

type OrderLineReset(key) =
    inherit ApplicationEvent(key)
    
type OrderLineProductAdded(key) =
    inherit ApplicationEvent(key)
    
type OrderLineProductRemoved(key) =
    inherit ApplicationEvent(key)
    
    
// And we have mappings from event to operation. There's a lot of possible duplication in these events, and some helper
// functions might greatly help maintaining the application. As it's only functions, we can create higher order functions,
// that is functions which creates functions.

// Operate only on event of a specific kind
let onEventOptional<'ev, 'op when 'ev :> ApplicationEvent> ctor (handler : ('ev -> 'op option)) : Handler = fun _ ev ->
    if ev :? 'ev then
        handler (ev :?> 'ev)
        |> Option.map ctor
    else
        None
    
let onEvent<'ev, 'op when 'ev :> ApplicationEvent> ctor (handler : ('ev -> 'op)) : Handler = fun source ->
    onEventOptional<'ev, _> ctor (handler >> Some) source
    
// As our language is complex and maps the events, the mapping from events to operations is straight forward.
// In practice, our language will be small and to the point, and the mappings will contain knowledge of the application.
// We've found that we often have several handlers for some events, and some needs to query data.
let handlers : Handler list = [
    onEvent<OrderLineCreated, _>
        Set
        (fun ev -> (ev.Key, defaultInitial))
    onEvent<OrderLineWithInitialValueCreated, _>
        Set
        (fun ev -> (ev.Key, ev.Value))
    onEvent<OrderLineReset, _>
        Reset
        (fun ev -> ev.Key)
    onEvent<OrderLineProductAdded, _>
        Incr
        (fun ev -> ev.Key)
    onEvent<OrderLineProductRemoved, _>
        Decr
        (fun ev -> ev.Key)
]

let demo () =
    printfn "Demo Small"
    printfn "=========="
    let events : ApplicationEvent list =
        [
            OrderLineCreated 1 // 1
            OrderLineProductAdded 1 // 2
            
            OrderLineWithInitialValueCreated (2, 2)
            OrderLineProductAdded 2 // 3
            OrderLineReset 2 // 2
        ]
        
    printfn "Processing application events: %A" events
        
    let oldState = State.Empty
    let newState =
        events
        |> Seq.fold (fun state ev -> handle handlers ev state) oldState
    let newState = persist newState
    printfn "State: %A" newState
    
    let oldState = newState
    let events : ApplicationEvent list =
        [
            OrderLineProductRemoved 2 // 1
        ]
        
    printfn ""
    printfn "Processing application events: %A" events
    let newState = 
        events
        |> Seq.fold (fun state ev -> handle handlers ev state) oldState
    let newState = persist newState
    printfn "Old state: %A" oldState
    printfn "New state: %A" newState
