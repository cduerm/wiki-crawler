package main

import (
  "fmt"
  "log"
  "time"
  "sort"
  "sync"
  "flag"
  "golang.org/x/net/html"
  "net/http"
)

// default links to search in german Wikipedia
var randomLink = "/wiki/Spezial:Zufällige_Seite"
var baseLink = "https://de.wikipedia.org"

// Commandline flags available
var nPages = flag.Int("nPages", 100, "how many random pages to query")
var showOutput = flag.Bool("showOutput", false, "show current pages (not recommended with go > 1)")
var request = flag.String("request", "", "request only this page, everythign else is ignored")
var follow = flag.String("follow", "", "follow links starting on this page, everythign else is ignored")

// struct to build a tree of visited pages and how many
// they were traversed
type Page struct {
  Title string
  Child *Page
  Parents []*Page
  Counter int
}

var visited map[string]*Page // map of all visited pages
var rwmutex = &sync.Mutex{} // Mutex to avoid read/write conflict
var wg sync.WaitGroup // WaitGroup to wait for all
//requests to be finished

// Functions to sort a slice of Pages by their Counter value
type ByCount []*Page
func (a ByCount) Len() int { return len(a) }
func (a ByCount) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a ByCount) Less(i, j int) bool { return a[i].Counter < a[j].Counter }

// findContentNode returns the node of the div-container enclosing
// the content text of a Wikipedia article
func findContentNode(n *html.Node) *html.Node {
  if n.Type == html.ElementNode && n.Data == "div" {
    for _, a := range n.Attr {
      if a.Key == "id" && a.Val == "mw-content-text" {
        return n
      }
    }
  }
  for c := n.FirstChild; c != nil; c = c.NextSibling {
    res := findContentNode(c)
    if res != nil {
      return res
    }
  }
  return nil
}

// findFirstLink takes an html.Node pointing to the content
// div-container of a Wikipedia article and returns the first
// internal Wikipedia link to another article which is not in any
// html-Tag and not in brackets.
func findFirstLink(n *html.Node) string {
  balance := 0
  for a := n.FirstChild; a != nil; a = a.NextSibling {
    if a.Type == html.ElementNode && ( a.Data == "p" || a.Data == "li" ) {
      for b := a.FirstChild; b != nil; b = b.NextSibling {
        if b.Type == html.TextNode {
          for _, s := range b.Data {
            switch s {
            case '(', '[':
              balance++
            case ')', ']':
              balance--
            }
          }
        } else if b.Type == html.ElementNode && b.Data == "a" && balance == 0 {
          for _, c := range b.Attr {
            if c.Key == "href" && len(c.Val) > 6 && c.Val[:6] == "/wiki/"{
              for _, s := range []string{"Spezial:","Benutzer:","Wikipedia:","File:","Datei:"} {
                if len(c.Val[6:]) > len(s) + 1 {
                  if c.Val[6:6 + len(s)] == s {
                    continue
                  }
                }
              }
              return c.Val
            }
          }
        }
      }
    }
    balance = 0
  }
  for a := n.FirstChild; a != nil; a = a.NextSibling {
    if a.Type == html.ElementNode {
      switch a.Data {
      case "div", "span", "ul", "ol":
        if a.Data == "div" && len(a.Attr) > 0 {
          continue
        }
        res := findFirstLink(a)
        if res != "/wiki/NOT_FOUND" {
          return res
        }
      }
    }
  }
  return "/wiki/NOT_FOUND"
}

// findTitle finds the title of a Wikipedia article given as
// a html.Node.
func findTitle(n *html.Node) string {
  if n.Type == html.ElementNode && n.Data == "h1" {
    return n.FirstChild.Data
  }
  for c := n.FirstChild; c != nil; c = c.NextSibling {
    res := findTitle(c)
    if res != "" {
      return res
    }
  }
  return ""
}

// parsePage takes an URL to a Wikipedia page and returns its
// title and the first link.
func parsePage(url string) (title, link string){
  doc, err := http.Get(url)

  if err != nil { log.Fatal(err) }

  page, err := html.Parse(doc.Body)
  if err != nil { log.Fatal(err) }

  title = findTitle(page)

  contentNode := findContentNode(page)
  if contentNode == nil {
    fmt.Println("not found")
    return title, ""
  }

  link = findFirstLink(contentNode)
  if *showOutput {
    url := doc.Request.URL
    fmt.Println(url)
    fmt.Printf("  title: %s \n  link:  %s\n", title, link)
  }

  return title, link
}

// increment All increments the Counters of already visited pages
// until reaching a loop.
func incrementAll(title, prevTitle string) {
  recentlyVisited := make(map [string]bool)
  visited[prevTitle].Child = visited[title]
  visited[title].Counter++
  visited[title].Parents = append(visited[title].Parents, visited[prevTitle])
  recentlyVisited[title] = true

  for {
    prevTitle = title
    var t *Page
    for {
      t = visited[title].Child
      if t != nil {
        title = t.Title
        break
      }
      rwmutex.Unlock()
      time.Sleep(500*time.Millisecond)
      rwmutex.Lock()
    }

    if recentlyVisited[title] {
      break
    }
    visited[title].Counter++
    recentlyVisited[title] = true
    //runtime.Gosched()
  }
  //rwmutex.Unlock()
  return
}

// followPage parses a given or otherwise random page and follows
// its links until an already visited page is found. Visited pages
// have their Counter increased and, if not already existing, are
// stored including Child and Parent pages in the global map visited.
func followPage(url ...string) {
  var prevTitle, title, link string
  if len(url) == 0 {
    title, link = parsePage(baseLink + randomLink)
  } else {
    title, link = parsePage(url[0])
  }
  rwmutex.Lock()
  _, exists := visited[title]
  if exists {
    rwmutex.Unlock()
    return
  }
  visited[title] = new(Page)
  visited[title].Title = title
  visited[title].Counter++
  rwmutex.Unlock()
  if *showOutput {
    fmt.Println(title)
  }

  for {
    prevTitle = title
    title, link = parsePage(baseLink + link)
    rwmutex.Lock()
    _, exists = visited[title]
    if exists {
      incrementAll(title, prevTitle)
      rwmutex.Unlock()
      return
    }
    visited[title] = new(Page)
    visited[title].Title = title
    visited[prevTitle].Child = visited[title]
    visited[title].Parents =  append(visited[title].Parents, visited[prevTitle])
    visited[title].Counter++
    rwmutex.Unlock()
  }
}

func main() {
  visited = make(map[string]*Page) // Initialize map of visited pages

  flag.Parse() // Parse commandline flags and assign thenm

  if *request != "" { // Just return the parse results of one page
    *showOutput = true
    parsePage(*request)
    return
  }

  if *follow != "" { // Just follow links starting on given page
    *showOutput = true
    followPage(*follow)
  }

  // Start goroutine for each random page to follow and wait
  // until all are finished
  for i := 0; i < *nPages; i++ {
    wg.Add(1)
    go func() {
      followPage()
      wg.Done()
    }()
  }
  wg.Wait()

  // Make list of all visited pages and sort it by Counter
  allPages := make([]*Page,len(visited))
  i := 0
  for _, val := range visited {
    allPages[i] = val
    i++
  }
  sort.Sort(ByCount(allPages))

  // Print all visited pages including Counter and Child
  for _, val := range(allPages) {
    fmt.Printf("%10d\t%s\t%s\n", val.Counter, val.Title, val.Child.Title)
  }
}
