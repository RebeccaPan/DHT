package main

import (
	"chord"
	"fmt"
)

func HelpInfo() {
	fmt.Println("- To put a <K, V> pair: [Put key value] e.g. Put ThisIsKeyText ThisIsValueText")
	fmt.Println("- To get a <K, V> pair: [Get key]       e.g. Get Rouge Info")
	fmt.Println("- To delete:     [Delete key]  e.g. Delete RougeInfo")
	fmt.Println("- To join:       [Join portID] e.g. Join 10086")
	fmt.Println("- To quit:       [Quit]")
	fmt.Println("- To force quit: [ForceQuit]")
	fmt.Println("- You can also [Ping addr], [Dump], or [Exit]")
}

func Init() {
	_, _ = red.Println("Welcome to the 'Rouge' Prototype of a DHT Application.")
	HelpInfo()
	hintMessage()
}

func hintMessage() {
	fmt.Println("--------------------")
	fmt.Print("Please input command:\n", port, ": ")
}

var op, key, val, str string
var IP = chord.LocAddr()
var port = 10000
var working = true
var done = false
var temInt int
var cur dhtNode

func mainWindow() {
	Init()
	cur = NewNode(port)
	cur.Run()
	cur.Create()
	nodeCnt := 1
	cur.Put("RougeInfo", "Rouge is a platform where you can store all kinds of lovely Rouge things.")
	cur.Put("LeRougeEtLeNoir", "https://www.bilibili.com/bangumi/play/ep334875?theme=movie")
	cur.Put("Rouge", "Rouge by RebeccaPan")
	for working {
		_, _ = fmt.Scan(&op)
		switch op {
		case "Help", "help":
			{
				HelpInfo()
			}
		case "Port", "port":
			{
				_, _ = fmt.Scan(&temInt)
				if temInt < 0 {
					fmt.Println("Port failure: invalid input")
				} else {
					port = temInt
					cur = NewNode(port)
					cur.Run()
					fmt.Println("Port success")
				}
			}
		case "Put", "put":
			{
				_, _ = fmt.Scan(&key, &val)
				done = cur.Put(key, val)
				if done {
					_, _ = yellow.Println("key =", key, "; val =", val)
					fmt.Println("Put success")
				} else {
					fmt.Println("Put failure")
				}
			}
		case "Get", "get":
			{
				_, _ = fmt.Scan(&key)
				done, val := cur.Get(key)
				if done {
					_, _ = yellow.Println(val)
					fmt.Println("Get success")
				} else {
					fmt.Println("Get failure")
				}
			}
		case "Delete", "delete":
			{
				_, _ = fmt.Scan(&key)
				done, val := cur.Get(key)
				if !done {
					fmt.Println("Delete Failure: key not found")
				} else {
					done = cur.Delete(key)
					if done {
						_, _ = yellow.Println("key =", key, "; val =", val)
						fmt.Println("Delete success")
					} else {
						fmt.Println("Delete failure")
					}
				}
			}
		case "Join", "join":
			{
				_, _ = fmt.Scan(&temInt)
				if nodeCnt == 0 {
					cur.Create()
				} else {
					fmt.Println(portToAddr(IP, temInt))
					done = cur.Join(portToAddr(IP, temInt))
				}
				if done {
					fmt.Println("Join success")
				} else {
					fmt.Println("Join failure")
				}
				nodeCnt++
			}
		case "Quit", "quit":
			{
				cur.Quit()
				fmt.Println("Quit success")
				nodeCnt--
			}
		case "ForceQuit", "force quit":
			{
				cur.ForceQuit()
				fmt.Println("ForceQuit success")
				nodeCnt--
			}
		case "Ping", "ping":
			{
				_, _ = fmt.Scan(&str)
				yellow.Println(cur.Ping(IP + ":" + str))
				fmt.Println("Ping success")
			}
		case "Dump", "dump":
			{
				cur.Dump()
			}
		case "Exit", "exit":
			{
				working = false
			}
		}
		if working {
			hintMessage()
		}
	}
	red.Println("Thanks for using Rouge.")
}
