package main

import (
  "fmt"
  "log"
  //"strings"
  //"github.com/PuerkitoBio/goquery"
  "time"
  "sort"
  "sync"
  "flag"
  "golang.org/x/net/html"
  "net/http"
)

var randomLink = "/wiki/Spezial:Zufällige_Seite"
var baseLink = "https://de.wikipedia.org"
var output = false

var nPages = flag.Int("nPages", 100, "how many random pages to query")
var nGos = flag.Int("nGos", 10, "how many parallel goroutines")
var showOutput = flag.Bool("showOutput", false, "show current pages (not recommended with go > 1)")
var request = flag.String("request", "", "request only this page, everythign else is ignored")

type Page struct {
  Title string
  Child *Page
  Parents []*Page
  Counter int
}

var visited map[string]*Page
var rwmutex = &sync.Mutex{}

type ByCount []*Page
func (a ByCount) Len() int { return len(a) }
func (a ByCount) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a ByCount) Less(i, j int) bool { return a[i].Counter < a[j].Counter }

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
  if output {
    url := doc.Request.URL
    fmt.Println(url)
    fmt.Printf("  title: %s \n  link:  %s\n", title, link)
  }

  return title, link
}

func incrementAll(title, prevTitle string) {
  _ = "breakpoint"
  //rwmutex.Lock()
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

func followPage() {
  var prevTitle string
  title, link := parsePage(baseLink + randomLink)
  rwmutex.Lock()
  _, exists := visited[title]
  //rwmutex.RUnlock()
  if exists {
    rwmutex.Unlock()
    return
  }
  //rwmutex.Lock()
  visited[title] = new(Page)
  visited[title].Title = title
  visited[title].Counter++
  rwmutex.Unlock()
  if output {
    fmt.Println(title)
  }

  for {
    prevTitle = title
    title, link = parsePage(baseLink + link)
    rwmutex.Lock()
    _, exists = visited[title]
    if exists {
      //rwmutex.Unlock()
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
  flag.Parse()

  // uncomment to search in english Wikipedia
  //baseLink = "https://en.wikipedia.org"
  //randomLink = "/wiki/Special:Random"

  visited = make(map[string]*Page)
  output = *showOutput

  if *request != "" {
    output = true
    parsePage(*request)
    return
  }

  //parsePage("https://de.wikipedia.org/wiki/1526")

  maxCount := *nPages
  curCount := 0
  maxGos := *nGos
  curGos := 0
  stopChan := make(chan int)
  for i := 0 ; i < maxGos ; i++ {
    if i >= maxCount {
      break
    }
    curCount++
    curGos++
    go func() {
      followPage()
      stopChan <- 1
    }()
  }
  var i int
  golauncher:
  for  {
    if curCount >= maxCount {
      break golauncher
    }
    select {
    case i = <-stopChan:
      curCount += i
      curGos--
      go func() {
        followPage()
        stopChan <- 1
      }()
      curGos++
    }
  }
  for curGos > 0 {
    <- stopChan
    curGos--
  }
  log.Println()

  allPages := make([]*Page,len(visited))
  i = 0
  for _, val := range visited {
    allPages[i] = val
    i++
  }
  sort.Sort(ByCount(allPages))
  for _, val := range(allPages) {
    fmt.Printf("%10d\t%s\t%s\n", val.Counter, val.Title, val.Child.Title)
  }
}
