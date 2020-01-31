package main

// STL only
import (
	"log"
	"crypto/md5"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"math/rand"
	"time"
	"sync"
)

// Global Variables
var (
	path = flag.String("path", "c:\\Windows", "Directory path")
	interval = flag.Duration("interval", 6, "Interval time in seconds")
	br *BrokerEX
	fx *DiffEX
)

// Enum for File State
type fileState string

// File State, OK -> No changes
const (
	OK fileState = "OK"
	NEW = "NEW"	
	MOD = "MOD"
	DEL = "DEL"
)

// File watch struct 
type FileEX struct {
	FileID string
	Name string
	Modified time.Time
	Hash string
	Check string 
	State fileState 
	Changed bool
}

func NewFileWatch(name string, check string, mtime time.Time) *FileEX {
	// We Generate unique file ID using name and last modification time
	fi := fmt.Sprintf("%s:%d", name, mtime.Unix())
	
	return &FileEX {
		// MD5 hash using file name
		FileID: fmt.Sprintf("%x", md5.Sum([]byte(name)))[:10], 
		Name: name,
		Modified: mtime,
		Check: check,
		// MD5 hash file ID to control changes
		Hash: fmt.Sprintf("%x", md5.Sum([]byte(fi))), 
		State: NEW,
		Changed: false,
	}
}

func (ob *FileEX) GetTime() string {
	return ob.Modified.Format("02/01/2006 15:04:05")
}

// Diff struct process, using sync.map to avoid race conditions
type DiffEX struct {
	Files sync.Map
	Check string
}

func NewDiffEX() *DiffEX {
	return &DiffEX{
		Check: GetID(),
	}
}

func (ob *DiffEX) AddWatch(fi *FileEX) {
	
	if ival, ok := ob.Files.Load(fi.FileID); ok {	
		
		val, _ := ival.(FileEX)
		
		if val.Hash != fi.Hash {
			fi.State = MOD
			fi.Changed = true
		} else{
			fi.State = OK
		}
	}
	ob.Files.Store(fi.FileID, *fi)
}

func (ob *DiffEX) Clean(){
	
	ob.Files.Range(func(key interface{}, ival interface{}) bool {
		
		val, _ := ival.(FileEX)
		
		if ob.Check != val.Check {
			
			val.State = DEL
			
			if bi, err := json.Marshal(val); err == nil {
				// New Event is created and published to broker
				ev := NewEvent(val.Name, bi)
				br.Publish("watch", *ev)
 			}
			
			ob.Files.Delete(key)
		}
		
		return true
    })
}

// Method to read files and publish events to broker
func (ob *DiffEX) CheckPath(path string) {
	
	// Perform clean to control Deleted files
	ob.Clean()

	// Generate check Hash ID
	ob.Check = GetID()
	
	err := filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
	
		if err != nil{
			return err
		}
		
		// We skip directories
		if info.IsDir() {
			return nil
		}
		
		// We control files using file name and last modified size 
		fi := NewFileWatch(info.Name(), ob.Check, info.ModTime())
		//fi.State = ob.AddWatch(*fi)
		ob.AddWatch(fi)
		
		switch fi.State {
			// File wasnt modified
			case "OK": {
				return nil
			}
			
			// File name or size has changes or is new
			default: {
				// Serializing FileEX data
				if bi, err := json.Marshal(fi); err != nil {
					return err
				} else{
					// New Event is created and published to broker
					ev := NewEvent(fi.Name, bi)
					br.Publish("watch", *ev)
				}
			}
		}
		
		return nil
		})
	
	if err != nil{
		log.Printf("Error %q: %v\n", path, err)
	}	
}

// Event struct for Broker 
// Can handle generic binary data 
type EventEX struct {
	EventID string
	Topic string
	Data []byte
	Timestamp time.Time
}

func NewEvent(topic string, data []byte) *EventEX {
	
	return &EventEX{
		EventID: GetID(),
		Topic: topic,
		Data: data,	
		Timestamp: time.Now(),
	}
}

// Type to handle generic functions that use EventEX struct
type ActionEX func(string, EventEX)

// Broker struct
type BrokerEX struct {
	BrokerID string
	Events map[string][]EventEX
	sync.RWMutex
}

func NewBroker() *BrokerEX {
	return &BrokerEX{
		BrokerID: GetID(),
		Events: make(map[string][]EventEX),
	}
}

// Subscribe method
// We use mutex locks to avoid race conditions

func (b *BrokerEX) Subscribe(topic string, ac ActionEX, interval time.Duration){
	for {
		b.RLock()		
		for _, x := range br.Events[topic] {
			go ac(topic, x)
		}
		b.RUnlock()
	
		b.Clean(topic)
	
		time.Sleep(interval * time.Millisecond)
	}
}

func (b *BrokerEX) Clean(topic string){
	b.Lock()
	br.Events[topic] = nil
	defer b.Unlock()	
}

// Publish method 
func (b *BrokerEX) Publish(topic string, ev EventEX){
	b.Lock()
    b.Events[topic] = append(b.Events[topic], ev)
	defer b.Unlock()	
}

func main(){
	flag.Parse()
	
	// We create new broker
	br = NewBroker()
	
	// Create new test event and publish
	ev := NewEvent("test", []byte("test"))
	br.Publish("test", *ev)
	
	// Create subscriptions, using a Token ID and a function to handle events
	go br.Subscribe("test", Logger, *interval)
	go br.Subscribe("watch", Logger, *interval)	

	// We create a diff file watcher 
	dd := NewDiffEX()
	
	// We start the file watcher 
	for {
		go dd.CheckPath(*path)		
		time.Sleep(*interval * time.Second)
	}
}

// Function that handles published events 
func Logger(topic string, ev EventEX){
	var msg string
	
	// We deserialize data based upon topic type 
	switch(topic){
		case "watch":{
			var data FileEX
			if err := json.Unmarshal(ev.Data, &data); err != nil {
				log.Println(err.Error())
			}
			
			msg = fmt.Sprintf("- File: %s \t Modified: %s \t State: %s", 
				data.Name, data.GetTime(), data.State)
		}
		break
		
		default:{
			msg = fmt.Sprintf("Topic: %s - Data: %s", topic, ev)
		}
	}
	log.Println(msg)
}

// Util 
func GetID() string {
	cr := make([]byte, 5)
	if _, err := rand.Read(cr); err != nil {
		panic(err)
	}
	return fmt.Sprintf("%X", cr)
}
