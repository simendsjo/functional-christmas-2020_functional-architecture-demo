// See README.org for a more thorough introduction
module Large

open System

[<AutoOpen>]
module ExternalApplication =
    // These types actually resides in our external application.
    // They are not very important for this example, so they are stubs
    type EntityType = string
    type EntityId = string
    type EntityRef = (EntityType * EntityId)

    type ApplicationEvent(entity) =
        member val Entity : EntityId = entity
        
    // In this example, we only have InactiveManually/ActiveManually, which translates to false/true.
    // I'm adding this type here for completeness in the discussions although we don't use it.
    type ThisTimelineDetailedState =
        // This should only be used as an intermediate status while computing the actual status
        | Inconclusive = 0
        // An inactive state has been made active directly.
        | ActiveManually = 1
        /// Inactivated manually. The previous manual state is Active.
        | InactiveManually = 2
        /// A dependency has made an active state inactive.
        | InactiveByDependency = 3
        /// A dependency has reactivated a previously inactivated state.
        | ActiveByDependency = 4
    
    // This is a representation of a timeline, in all it's calculated glory
    type Timeline = {
        Id : EntityRef
        // .. omitting actual implementation, and just adding some data to print something
        Items : (DateTime * string) list
    }
 
/////////////////////////////////////////////
// So this is where our application begins //
/////////////////////////////////////////////

// We start by creating some aliases to show intent. It's possible to create single case discriminated unions for
// instance, but aliases is fine for simple use
type TimelineId = EntityRef

type Child = TimelineId
type Parent = TimelineId

// A relation between two timelines
type Dependency = {
    Child : TimelineId
    Parent : TimelineId
    // In order to give a description on the type or relation
    // An Order might be owned by a User, add on behalf of another
    // User, verified by a third User and so on.
    Relation : string
}

type TimelineItemId = string
// A single state in a timeline
type TimelineItem = {
    // Id to modify or delete an item
    Key : TimelineItemId
    State : ThisTimelineDetailedState
    From : DateTime
    // A description for this item, "New", "Created" etc.
    // Sent as information to the timelines
    Item : string
}

// Operations that manipulate the state
type Operation =
    // Entire timeline and all dependencies will be deleted
    | DeleteTimeline of TimelineId
    // Adds a dependency. Child and everything dependent on it will be marked as dirty
    | AddDependency of Dependency
    // Removes a dependency. Child and everything dependent on it will be marked as dirty
    | RemoveDependency of Dependency
    // Adds or updates a timeline item. The timeline and everything dependent on it will be marked as dirty
    | SetTimelineItem of (TimelineId * TimelineItem)
    // Removes a timeline item. The timeline and everything dependent on it will be marked as dirty
    | DeleteTimelineItem of (TimelineId * TimelineItemId)


// Some change in state
type Effect = (ApplicationEvent * Operation)

// Entire state for the application
type State = {
    // All items
    ThisTimeline : Map<TimelineId, TimelineItem list>
    
    // Dependencies from two directions for fast lookups
    DependenciesFor : Map<Child, Dependency list>
    DependentOn : Map<Parent, Dependency list>
    
    // An audit of the current changes, think of it
    // as a write-ahead log used by transactions and
    // interpreters like database persisting
    Executed : Effect list
    
    // Various state here
    Calculated : Map<TimelineId, Timeline>
    
    // "Dirty" timelines which must be calculated before persisted
    // or shown to the user
    NeedsRecalculation : Set<TimelineId>
    
    // State for the database interpreted
    Unpersisted : Set<TimelineId>
    
    // ... and of course the actual timelines which we don't care about in this example
} with
    static member Empty : State = {
        ThisTimeline = Map.empty
        DependenciesFor = Map.empty
        DependentOn = Map.empty
        Calculated = Map.empty
        Executed = []
        NeedsRecalculation = Set.empty
        Unpersisted = Set.empty
    }

    
// In the actual implementation, we have all state things in a separate file, and most functions being private
// We'll just add it to a module here to keep everything in a single file and to show that very little is exposed
[<AutoOpen>]
module StateManipulation =
    let private removeValue k v map =
        let withoutValue =
            Map.tryFind k map
            |> Option.defaultValue []
            |> List.filter ((<>) v)
        if List.isEmpty withoutValue then
            // Avoid having both None and []
            // meaning the same thing
            Map.remove k map
        else
            Map.add k withoutValue map
            
    let private insertValue k v map =
        Map.add k (v :: (Map.tryFind k map |> Option.defaultValue [])) map
            
    let removeCache (tl : TimelineId) (state : State) : State =
        { state with
            Calculated = Map.remove tl state.Calculated
            // .. and clears other caches
        }
        
    let rec private deepGetDependentOn (parent : Parent) (state : State) : Dependency list =
        Map.tryFind parent state.DependentOn
        |> Option.defaultValue []
        |> List.fold (fun allDeps dep -> [dep] :: (deepGetDependentOn dep.Child state) :: allDeps) []
        |> List.concat
        |> List.distinct

    // Whenever something is modified, we need to mark everything as dirty. It's turtles all the way down.
    let rec private markDirty (tl : TimelineId) (state : State) : State =
        deepGetDependentOn tl state
        |> Seq.map (fun dep -> dep.Child)
        |> Seq.append [tl]
        |> Seq.fold (fun state tl ->
            { state with
                NeedsRecalculation = Set.add tl state.NeedsRecalculation
                Unpersisted = Set.add tl state.Unpersisted
            }
            |> removeCache tl
        ) state
        
    let private addDependency (dep : Dependency) (state : State) : State =
        { state with
            DependenciesFor = insertValue dep.Child dep state.DependenciesFor
            DependentOn = insertValue dep.Parent dep state.DependentOn
        }
        |> markDirty dep.Child
        
    let private deepDeleteTimelines (tl : TimelineId) (state : State) : State =
        // Delete everything dependant on this timeline
        deepGetDependentOn tl state
        // Delete this timeline even though it doesn't have any dependencies
        // We add it as a fictional dependency to reuse the other code
        |> List.append [{Child = tl; Parent = tl; Relation = "Self"}]
        // Delete this the dependency references for this timeline
        |> List.append (Map.tryFind tl state.DependenciesFor |> Option.defaultValue [])
        // Actually delete dependencies and timelines
        |> List.fold (fun state dep ->
            { state with
                ThisTimeline = Map.remove dep.Child state.ThisTimeline
                DependenciesFor = removeValue dep.Child dep state.DependenciesFor
                DependentOn = removeValue dep.Parent dep state.DependentOn
            }
            |> removeCache dep.Child
            |> fun state ->
                { state with
                    // Mark that the timeline is changed, but don't recalculate an empty timeline as we've deleted it
                    Unpersisted = Set.add dep.Child state.Unpersisted
                    NeedsRecalculation = Set.remove dep.Child state.NeedsRecalculation
                }
        ) state

    let removeDependency (dep : Dependency) (state : State) : State =
        { state with
            DependenciesFor = removeValue dep.Child dep state.DependenciesFor
            DependentOn = removeValue dep.Parent dep state.DependentOn
        }
        |> markDirty dep.Child
        
    let private deleteTimelineItem ((tlId, item) : (TimelineId*TimelineItemId)) (state : State) : (State * TimelineId) =
        let tl =
            Map.tryFind tlId state.ThisTimeline
            |> Option.defaultValue ([])
            |> List.filter (fun x -> x.Key <> item)
        { state with ThisTimeline = Map.add tlId tl state.ThisTimeline }
        |> markDirty tlId
        |> fun state -> (state, tlId)
        
    let private setTimelineItem ((tlId, item) : (TimelineId*TimelineItem)) (state : State) : (State * TimelineId) =
        let tl =
            Map.tryFind tlId state.ThisTimeline
            |> Option.defaultValue ([])
            |> List.filter (fun x -> x.Key <> item.Key)
            |> fun pre -> List.append pre [item]

        { state with ThisTimeline = Map.add tlId tl state.ThisTimeline }
        |> markDirty tlId
        |> fun state -> (state, tlId)
        
    let rec deepGetDependenciesFor (child : Child) (state : State) : Dependency list =
        Map.tryFind child state.DependenciesFor
        |> Option.defaultValue []
        |> List.fold (fun allDeps dep -> [dep] :: (deepGetDependenciesFor dep.Parent state) :: allDeps) []
        |> List.concat
        |> List.distinct
        
    let execute (op : Operation) (state : State) : (State * Operation list) =
        match op with
        | DeleteTimeline tl ->
            let tls =
                let children = deepGetDependentOn tl state |> List.map (fun dep -> dep.Child)
                tl :: children
            let state = deepDeleteTimelines tl state
            let ops = tls |> List.map DeleteTimeline
            (state, ops)
        | AddDependency dep ->
            (addDependency dep state, [op])
        | RemoveDependency dep ->
            (removeDependency dep state, [op])
        | SetTimelineItem (tl, item) ->
            let state, tl = setTimelineItem (tl, item) state
            (state, [(SetTimelineItem (tl, item))])
        | DeleteTimelineItem (tl, item) ->
            let state, tl = deleteTimelineItem (tl, item) state
            (state, [(DeleteTimelineItem (tl, item))])
        

type Handler = State -> ApplicationEvent -> Operation option

let handle (handlers : Handler list) (event : ApplicationEvent) (state : State) : State =
    handlers
    |> Seq.fold (fun state handler ->
        handler state event
        |> Option.map (fun op ->
            execute op state
            ||> Seq.fold (fun state op ->
                { state with
                    Executed = (event, op) :: state.Executed
                    // In our actual implementation, we're adding more things, like caching the number of operations
                    // in total so we can quickly do our stale data detection without resorting to a full diff
                    // of states
                }
            )
        )
        |> Option.defaultValue state
    ) state


// Logic to recalculate all state. We don't care about it here, so let's just pretend we're doing the work
// The implementation here is just to be more complete, and isn't interesting for our architecture discussion
// 
// Feel free to ignore this section!
[<AutoOpen>]
module TimelineCalculation =
    let private recalculateFromCache (tl : TimelineId) (state : State) : (Timeline * State) option =
        Map.tryFind tl state.Calculated
        |> Option.map (fun calculated -> (calculated, state))
        
    // This is where we're actually doing work. This is really very specific to our application, but I'm adding it here
    // just for completeness. Various parts has been removed and/or changed as we have a simplified model here.
    let private recalculateFromItems (tl : TimelineId) (_deps : Timeline seq) (state : State) : (Timeline * State) =
        let thisItems =
            Map.tryFind tl state.ThisTimeline
            |> Option.defaultValue []
        thisItems
        // Merge on active/inactive; allow a status to swallow subsequent statuses of the same type
        |> Seq.sortBy (fun x -> x.From)
        |> Seq.fold (fun (prevState, prevFrom, items) itm ->
            if itm.State = prevState then // Subsequent same status, ignore status
                (prevState, prevFrom, items)
            elif itm.From = prevFrom then // Multiple statuses on the same date, and they differ in active/non-active state! Should be fixed, but replace previous item with this new registration
                (itm.State, itm.From, itm :: (List.tail items))
            else (itm.State, itm.From, itm :: items)
        ) (ThisTimelineDetailedState.Inconclusive, DateTime.MaxValue, [])
        |> fun (_, _, xs) -> xs
        // Calculate
        |> List.map (fun i -> (i.From, i.Item))
        |> fun items ->
            let calculated =
                { Timeline.Id = tl; Items = items }
            let state =
                // We no longer need the cached items as we have a calculated state
                removeCache tl state
                |> fun state -> { state with Calculated = Map.add tl calculated state.Calculated }
            (calculated, state)
        
    let private recalculateFromCachedItems (tl : TimelineId) (_deps : Timeline seq) (state : State) : (Timeline * State) option =
        let calculated =
            let items =
                Map.tryFind tl state.ThisTimeline
                |> Option.defaultValue []
                |> List.map (fun i -> (i.From, i.Item))
            // Calling the actual implementation with this data and dependencies would recalculate the timeline
            { Timeline.Id = tl; Items = items }
        let state =
            // We no longer need the cached items as we have a calculated state
            removeCache tl state
            |> fun state -> { state with Calculated = Map.add tl calculated state.Calculated }
        Some (calculated, state)
        
    // This is the way to get a timeline out of the state. As it might recalculate because things was dirty,
    // the called should store the new State to avoid doing unnecessary work when calling again.
    let rec calculateTimeline (tl : TimelineId) (state : State) : (Timeline * State) =
        recalculateFromCache tl state
        |> Option.defaultWith (fun () ->
            // Recalculated all dependencies
            let deps, state =
                deepGetDependenciesFor tl state
                |> Seq.fold (fun (deps, state) dep ->
                    let (tl, state) = calculateTimeline dep.Parent state
                    (tl :: deps, state)
                ) ([], state)
            let calculated, state =
                recalculateFromCachedItems tl deps state
                |> Option.defaultWith (fun () -> recalculateFromItems tl deps state)
            (calculated, state)
        )
        |> fun (calculated, state) ->
            (calculated, {state with NeedsRecalculation = Set.remove tl state.NeedsRecalculation})
        
    let calculate (state : State) : State =
        state.NeedsRecalculation
        |> Set.toSeq
        |> Seq.distinct
        |> Seq.fold (fun state tl -> calculateTimeline tl state |> snd) state


// Then there are the top-level application logic. This is our outer shell, and this is where the side-effects like
// saving to the database exist. Loading from the database and saving to the database is the only side-effect effect
// in our application. Loading is done at application startup, and persisting is done when a transaction is saved.
// Setting the new state as the current state is done in the application calling this code (notice that we just return
// the state)

// This is the handler called from our actual application in order to produce a new state.
let handleEvents (handlers : Handler list) (events : ApplicationEvent seq) (state : State) : State =
    events
    |> Seq.fold (fun state ev -> handle handlers ev state) state
    |> calculate

// We also store everything to a database in order to retrieve it at application start.
// Notice that we only look at the Executed effects, this is thus an interpretation of our language defined as
// Operation. This EDSL + Interpreter pattern is useful for keeping mutation out of your core application
let persistExecuted (state : State) : State =
    let inOrder = state.Executed |> Seq.rev |> Seq.toList
    let state =
        calculate state
        // Someone might have calculated something we want to delete manually
        // (for instance "calculate all affected timelines")
        // To avoid storing this in the database, we delete it from the cache
        // just in case
        |> fun state ->
            inOrder
            |> Seq.choose (function | (_, DeleteTimeline tl) -> Some tl | _ -> None)
            |> Seq.fold (fun state tl -> removeCache tl state) state

    // The actual implementation is highly optimized for writing fast to the database, but we'll just print
    // to simulate the side-effects
    inOrder |> Seq.iter (snd >> printfn "Storing to database: %A")
    
    // Storing to the database is the last thing we done when our transaction is done, so we'll also clear
    // the Executed state
    { state with
        Executed = []
        Unpersisted = Set.empty
    }


// So far, everything has been mostly infrastructure, and nothing specific to our application.
// Now we're going to write the actual code that does the mapping from ApplicationEvent to Operation,

type CompanyCreated(company, created) =
    inherit ApplicationEvent(company)
    member val Created : DateTime  = created

type CompanyDiscontinued(company, closed) =
    inherit ApplicationEvent(company)
    member val Closed : DateTime = closed

type AccountOpened(account, bank, owner, opened) =
    inherit ApplicationEvent(account)
    member val Owner : EntityId = owner
    member val Bank : EntityId = bank
    member val Opened : DateTime = opened

type AccountSuspended(account, suspended) =
    inherit ApplicationEvent(account)
    member val Suspended : DateTime = suspended
    
type AccountReopened(account, reopened) =
    inherit ApplicationEvent(account)
    member val Reopened : DateTime = reopened
    
type AccountClosed(account, closed) =
    inherit ApplicationEvent(account)
    member val Closed : DateTime = closed
    
type PersonCreated(person, birthday) =
    inherit ApplicationEvent(person)
    member val Birthday : DateTime = birthday
    
type PersonDied(person, timeOfDeath) =
    inherit ApplicationEvent(person)
    member val TimeOfDeath : DateTime = timeOfDeath
    
[<AutoOpen>]
module ApplicationEventHandlers =
    // These are our helper functions. Functions that create other functions.
    [<AutoOpen>]
    module private Helpers =
        // In our example, we notice a pattern where we only care about certain type of events.
        // Another pattern is that we always call a constructor from Operation.
        // This function wraps these two patterns.
        let onEventOptional<'ev, 'op when 'ev :> ApplicationEvent> ctor (handler : ('ev -> 'op option)) : Handler = fun _ ev ->
            if ev :? 'ev
            then handler (ev :?> 'ev) |> Option.map ctor
            else None

        // Often, an operation should always be created given an event. For this case, we don't need to have
        // an optional handler, but can have it just return the operation, and let us wrap it in a Some
        let onEvent<'ev, 'op when 'ev :> ApplicationEvent> ctor (handler : ('ev -> 'op)) : Handler = fun source ->
            onEventOptional<'ev, _> ctor (handler >> Some) source

        // Helper to create a dependency operation
        let onDependencyEvent<'ev when 'ev :> ApplicationEvent> ctor (relation : string) (extractChild : ('ev -> EntityRef)) (extractParent : ('ev -> EntityRef)) : Handler =
            onEvent<'ev, _> ctor (fun ev ->
                let dep = {
                    Child = extractChild ev
                    Parent = extractParent ev
                    Relation = relation
                }
                dep
            )

        // This is where we're creating functions that matches our domain specific language
        
        let deletesTimeline<'ev when 'ev :> ApplicationEvent> (extractTimelineId : ('ev -> TimelineId)) : Handler =
            onEvent<'ev, _> DeleteTimeline (fun ev ->
                extractTimelineId ev
            )

        let addsDependency<'ev when 'ev :> ApplicationEvent> =
            onDependencyEvent<'ev> AddDependency

        let removesDependency<'ev when 'ev :> ApplicationEvent> =
            onDependencyEvent<'ev> RemoveDependency

        // As these functions are abstractions, we can build logic into them. For instance can we say that
        // MaxDate should be interpreted as "not yet active", and we can patch small values to be at least of some
        // size (like a large enough value to not crash MSSQL)
        let minDate = DateTime(1753, 1, 2)
        let setsTimelineItem<'ev when 'ev :> ApplicationEvent> (f : ('ev -> (TimelineId*TimelineItem))) : Handler =
            onEventOptional<'ev, _> SetTimelineItem (fun ev ->
                let tl, itm = f ev
                if itm.From = DateTime.MaxValue
                then None
                else
                let itm =
                    if itm.From < minDate
                    then { itm with From = minDate }
                    else itm
                Some (tl, itm))

        let deletesTimelineItem<'ev when 'ev :> ApplicationEvent> (f : ('ev -> (TimelineId*TimelineItemId))) : Handler =
            onEvent<'ev, _> DeleteTimelineItem (fun ev ->
                let tl, itm = f ev
                (tl, itm))
            
            
        // As this is only functions creating other functions, we can easily drop down to lower levels when needed,
        // or we can build ever larger abstractions. If Handler was changed to return a list of Operations instead,
        // we could have functions that creates many operations with a single function.
        
    // Using the helpers, we can now create handlers for our events. The handlers should just be a list of Handler.
    // This gives us a lot of flexibility. The real implementation has one sublist per feature, and then concat them
    // together to create a complete list.
    let handlers : Handler list = [
        setsTimelineItem<CompanyCreated> (fun ev ->
            let tl = ("Company", ev.Entity)
            let item = {
                Key = "Created"
                State = ThisTimelineDetailedState.ActiveManually
                From = ev.Created
                Item = "Created"
            }
            (tl, item)
        )
        setsTimelineItem<CompanyDiscontinued> (fun ev ->
            let tl = ("Company", ev.Entity)
            let item = {
                Key = "Discontinued"
                State = ThisTimelineDetailedState.InactiveManually
                From = ev.Closed
                Item = "Discontinued"
            }
            (tl, item)
        )
        
        setsTimelineItem<PersonCreated> (fun ev ->
            let tl = ("Person", ev.Entity)
            let item = {
                Key = "Birthday"
                State = ThisTimelineDetailedState.ActiveManually
                From = ev.Birthday
                Item = "Birthday"
            }
            (tl, item)
        )
        
        setsTimelineItem<PersonDied> (fun ev ->
            let tl = ("Person", ev.Entity)
            let item = {
                Key = "Death"
                State = ThisTimelineDetailedState.InactiveManually
                From = ev.TimeOfDeath
                Item = "Death"
            }
            (tl, item)
        )
        
        addsDependency<AccountOpened> "Bank" (fun ev -> ("Account", ev.Entity)) (fun ev -> ("Bank", ev.Bank))
        addsDependency<AccountOpened> "Owner" (fun ev -> ("Account", ev.Entity)) (fun ev -> ("Person", ev.Owner))
        setsTimelineItem<AccountOpened> (fun ev ->
            let tl = ("Account", ev.Entity)
            let item = {
                Key = "Opened"
                State = ThisTimelineDetailedState.ActiveManually
                From = ev.Opened
                Item = "Opened"
            }
            (tl, item)
        )
        
        setsTimelineItem<AccountSuspended> (fun ev ->
            let tl = ("Account", ev.Entity)
            let item = {
                Key = sprintf "Suspended %A" ev.Suspended
                State = ThisTimelineDetailedState.InactiveManually
                From = ev.Suspended
                Item = "Suspended"
            }
            (tl, item)
        )
        
        setsTimelineItem<AccountReopened> (fun ev ->
            let tl = ("Account", ev.Entity)
            let item = {
                Key = sprintf "Reopened %A" ev.Reopened
                State = ThisTimelineDetailedState.ActiveManually
                From = ev.Reopened
                Item = "Reopened"
            }
            (tl, item)
        )
    ]

let demo () =
    printfn "Demo Large"
    printfn "=========="
    let events : ApplicationEvent list =
        [
            CompanyCreated ("The bank", DateTime(1984, 01, 01))
            PersonCreated ("Poor soul", DateTime(2020, 03, 13))
            AccountOpened ("42", "The bank", "Poor soul", DateTime(2020, 06, 01))
            AccountSuspended("42", DateTime(2020, 08, 01))
            AccountReopened("42", DateTime(2020, 10, 01))
        ]
        
    printfn "Processing application events: %A" events
        
    let oldState = State.Empty
    let newState =
        events
        |> Seq.fold (fun state ev -> handle handlers ev state) oldState
    let newState = persistExecuted newState
    printfn "State: %A" newState
    
    let oldState = newState
    let events : ApplicationEvent list =
        [
            CompanyDiscontinued ("The bank", DateTime(2020, 12, 01))
        ]
        
    printfn ""
    printfn "Processing application events: %A" events
    let newState = 
        events
        |> Seq.fold (fun state ev -> handle handlers ev state) oldState
    let newState = persistExecuted newState
    printfn "Old state: %A" oldState
    printfn "New state: %A" newState
